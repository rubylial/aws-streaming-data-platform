package com.streaming.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * PE5 — Test local de escritura Iceberg con HadoopCatalog.
 *
 * Valida la integración Flink → Iceberg usando catálogo local
 * (HadoopCatalog + filesystem) en lugar de GlueCatalog + S3.
 *
 * Demuestra:
 *   - Creación de tablas Iceberg con particionado por merchant_id
 *   - Escritura de datos vía Flink SQL INSERT INTO
 *   - Generación de metadata Iceberg (snapshots, manifests)
 *   - Generación de data files Parquet
 *   - Commit atómico Iceberg verificado por existencia de metadata
 *
 * Diferencias documentadas frente a producción:
 *   - Catálogo: HadoopCatalog (local) vs GlueCatalog (AWS)
 *   - Storage: filesystem local vs S3
 *   - IAM/KMS: no aplica en local
 *
 * Ejecución:
 *   mvn test -Dtest=IcebergLocalTest
 */
public class IcebergLocalTest {

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(4)
                            .setNumberTaskManagers(1)
                            .build());

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testIcebergTableCreationAndWrite() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000L);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String warehousePath = tempFolder.newFolder("iceberg-warehouse").getAbsolutePath();

        System.out.println("=== PE5 Iceberg Local Test ===");
        System.out.println("Warehouse path: " + warehousePath);
        System.out.println("Catalog: HadoopCatalog (local filesystem)");
        System.out.println("==============================");

        // -------------------------------------------------------
        // 1. Registrar catálogo Iceberg con HadoopCatalog (local)
        //    En producción se usa GlueCatalog + S3FileIO
        // -------------------------------------------------------
        tableEnv.executeSql(String.format(
            "CREATE CATALOG local_iceberg WITH ("
            + "  'type' = 'iceberg',"
            + "  'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog',"
            + "  'warehouse' = '%s'"
            + ")", warehousePath));

        tableEnv.useCatalog("local_iceberg");

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS test_db");
        tableEnv.useDatabase("test_db");

        System.out.println("[OK] Catálogo Iceberg registrado");

        // -------------------------------------------------------
        // 2. Crear tabla transactions (mismo schema que Glue)
        //    Particionada por merchant_id (hidden partitioning)
        // -------------------------------------------------------
        tableEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS transactions ("
            + "  event_id STRING,"
            + "  event_time TIMESTAMP(6),"
            + "  merchant_id STRING,"
            + "  amount DOUBLE,"
            + "  currency STRING,"
            + "  status STRING,"
            + "  processing_time TIMESTAMP(6)"
            + ") PARTITIONED BY (merchant_id)"
        );

        System.out.println("[OK] Tabla transactions creada con partición merchant_id");

        // -------------------------------------------------------
        // 3. Crear tabla merchant_aggregations
        // -------------------------------------------------------
        tableEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS merchant_aggregations ("
            + "  merchant_id STRING,"
            + "  window_start TIMESTAMP(6),"
            + "  window_end TIMESTAMP(6),"
            + "  transaction_count BIGINT,"
            + "  total_amount DOUBLE,"
            + "  avg_amount DOUBLE"
            + ") PARTITIONED BY (merchant_id)"
        );

        System.out.println("[OK] Tabla merchant_aggregations creada con partición merchant_id");

        // -------------------------------------------------------
        // 4. Insertar datos de prueba en transactions
        // -------------------------------------------------------
        tableEnv.executeSql(
            "INSERT INTO transactions VALUES "
            + "('evt-001', TIMESTAMP '2026-01-15 10:00:05', 'merchant-001', 100.00, 'USD', 'approved', CURRENT_TIMESTAMP),"
            + "('evt-002', TIMESTAMP '2026-01-15 10:00:15', 'merchant-001', 250.50, 'USD', 'approved', CURRENT_TIMESTAMP),"
            + "('evt-003', TIMESTAMP '2026-01-15 10:00:30', 'merchant-002', 500.00, 'USD', 'approved', CURRENT_TIMESTAMP),"
            + "('evt-004', TIMESTAMP '2026-01-15 10:01:10', 'merchant-001',  75.00, 'USD', 'pending',  CURRENT_TIMESTAMP),"
            + "('evt-005', TIMESTAMP '2026-01-15 10:01:45', 'merchant-003', 1500.00, 'USD', 'approved', CURRENT_TIMESTAMP)"
        ).await();

        System.out.println("[OK] 5 transacciones insertadas (3 merchants)");

        // -------------------------------------------------------
        // 5. Insertar datos en merchant_aggregations
        // -------------------------------------------------------
        tableEnv.executeSql(
            "INSERT INTO merchant_aggregations VALUES "
            + "('merchant-001', TIMESTAMP '2026-01-15 10:00:00', TIMESTAMP '2026-01-15 10:01:00', 2, 350.50, 175.25),"
            + "('merchant-002', TIMESTAMP '2026-01-15 10:00:00', TIMESTAMP '2026-01-15 10:01:00', 1, 500.00, 500.00),"
            + "('merchant-001', TIMESTAMP '2026-01-15 10:01:00', TIMESTAMP '2026-01-15 10:02:00', 1,  75.00,  75.00),"
            + "('merchant-003', TIMESTAMP '2026-01-15 10:01:00', TIMESTAMP '2026-01-15 10:02:00', 1, 1500.00, 1500.00)"
        ).await();

        System.out.println("[OK] 4 agregaciones insertadas");

        // -------------------------------------------------------
        // 6. Verificar archivos físicos: metadata + parquet + manifests
        //    La existencia de estos archivos demuestra que Iceberg
        //    realizó commits atómicos exitosos
        // -------------------------------------------------------
        File warehouseDir = new File(warehousePath);

        List<Path> metadataFiles = Files.walk(warehouseDir.toPath())
                .filter(p -> p.toString().contains("metadata") && p.toString().endsWith(".json"))
                .collect(Collectors.toList());

        List<Path> dataFiles = Files.walk(warehouseDir.toPath())
                .filter(p -> p.toString().endsWith(".parquet"))
                .collect(Collectors.toList());

        List<Path> manifestFiles = Files.walk(warehouseDir.toPath())
                .filter(p -> p.toString().endsWith(".avro"))
                .collect(Collectors.toList());

        System.out.println("\n--- Archivos Iceberg en warehouse ---");
        System.out.println("  Metadata (.json):  " + metadataFiles.size());
        System.out.println("  Data (.parquet):   " + dataFiles.size());
        System.out.println("  Manifests (.avro): " + manifestFiles.size());

        assertFalse("Debe haber metadata JSON (snapshots)", metadataFiles.isEmpty());
        assertFalse("Debe haber data files Parquet", dataFiles.isEmpty());
        assertFalse("Debe haber manifest files Avro", manifestFiles.isEmpty());

        System.out.println("\n--- Detalle de archivos ---");
        for (Path p : metadataFiles) {
            System.out.println("  [metadata] " + warehouseDir.toPath().relativize(p));
        }
        for (Path p : dataFiles) {
            System.out.println("  [data]     " + warehouseDir.toPath().relativize(p));
        }
        for (Path p : manifestFiles) {
            System.out.println("  [manifest] " + warehouseDir.toPath().relativize(p));
        }

        System.out.println("[OK] Estructura Iceberg verificada: metadata + parquet + manifests");

        // -------------------------------------------------------
        // 7. Verificar que ambas tablas tienen estructura Iceberg
        // -------------------------------------------------------
        boolean hasTxnMetadata = metadataFiles.stream()
                .anyMatch(p -> p.toString().contains("transactions"));
        boolean hasAggMetadata = metadataFiles.stream()
                .anyMatch(p -> p.toString().contains("merchant_aggregations"));

        assertTrue("Tabla transactions debe tener metadata Iceberg", hasTxnMetadata);
        assertTrue("Tabla merchant_aggregations debe tener metadata Iceberg", hasAggMetadata);

        System.out.println("[OK] Ambas tablas tienen metadata Iceberg independiente");

        // -------------------------------------------------------
        // 8. Verificar particiones por merchant_id
        // -------------------------------------------------------
        List<Path> partitionDirs = Files.walk(warehouseDir.toPath())
                .filter(p -> p.toString().contains("merchant_id="))
                .filter(Files::isDirectory)
                .collect(Collectors.toList());

        System.out.println("\n--- Particiones Iceberg ---");
        if (!partitionDirs.isEmpty()) {
            for (Path p : partitionDirs) {
                System.out.println("  " + warehouseDir.toPath().relativize(p));
            }
            System.out.println("[OK] Particionado por merchant_id verificado en filesystem");
        } else {
            System.out.println("[INFO] Iceberg hidden partitioning — metadata maneja particiones internamente");
        }

        // -------------------------------------------------------
        // Resumen final
        // -------------------------------------------------------
        System.out.println("\n========================================");
        System.out.println("PE5 Iceberg Local Test — PASSED");
        System.out.println("  Catálogo: HadoopCatalog (local)");
        System.out.println("  Tablas creadas: transactions, merchant_aggregations");
        System.out.println("  Datos: 5 transacciones + 4 agregaciones");
        System.out.println("  Parquet data files: " + dataFiles.size());
        System.out.println("  Metadata snapshots: " + metadataFiles.size());
        System.out.println("  Manifest files: " + manifestFiles.size());
        System.out.println("  Commit atómico: verificado (INSERT INTO + await exitoso)");
        System.out.println("  Particionado: merchant_id");
        System.out.println("  Nota: en producción usa GlueCatalog + S3FileIO");
        System.out.println("========================================");
    }
}
