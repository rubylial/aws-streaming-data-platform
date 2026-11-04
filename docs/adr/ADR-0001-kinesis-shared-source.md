# ADR-0001 — Kinesis como fuente compartida para Flink y Redshift

**Estado:** Proposed
**Fecha:** 2026-11-03
**Autor:** Ruby Alvarez

## Contexto

El proyecto requiere procesamiento stateful hacia Apache Iceberg (vía Managed Flink) y analítica in-stream con Redshift Streaming Ingestion. Ambas ramas consumen los mismos eventos del productor JSON.

## Decisión

Kinesis Data Streams es la fuente única de eventos. Tres consumidores independientes leen del mismo stream:

1. **Data Firehose** → S3 Bronze (landing raw, auditoría)
2. **Managed Apache Flink** → Apache Iceberg en S3 con Glue Catalog
3. **Redshift Streaming Ingestion** → Materialized View base

Cada consumidor mantiene su propio iterator/checkpoint. No hay acoplamiento directo entre ellos.

## Consecuencias

### Positivas
- Desacoplamiento: cada consumidor avanza a su propio ritmo
- Simplicidad operativa: un solo stream, un solo productor
- Contrato de eventos centralizado (`event_id`, `event_time`, `merchant_id`, `amount`, `currency`, `status`)

### Riesgos y mitigaciones
- **Throughput compartido**: cada shard soporta 2 MB/s de lectura divididos entre consumidores. Con 3 consumidores en 2 shards, cada uno obtiene ~666 KB/s. Monitorizar `ReadProvisionedThroughputExceeded` y `IteratorAgeMilliseconds`.
- **Enhanced fan-out**: asigna 2 MB/s exclusivos por consumidor registrado. Evaluar si el costo se justifica en un ambiente de desarrollo.
- **Retención**: el periodo de retención de Kinesis (por defecto 24h) debe ser suficiente para que todos los consumidores procesen eventos antes de su expiración.

### Aclaraciones importantes
- **Backpressure NO se propaga entre consumidores**: si Redshift tiene lag en el refresh de su MV, eso NO genera backpressure en Flink ni en Firehose. Son consumidores independientes que comparten la capacidad del stream.
- **Sin atomicidad transaccional**: un evento visible en Iceberg y en la MV de Redshift NO implica escritura atómica. Habrá ventanas de inconsistencia eventual. Reconciliar por `event_id` y medir freshness de cada ruta.

## Alternativas consideradas

1. **Streams separados** (un stream por consumidor): mayor costo y complejidad de productor.
2. **Stream secundario de Flink** (agregados): Flink escribe a un segundo stream que Redshift consume. Reduce latencia de Redshift pero agrega complejidad.
3. **SNS fan-out a múltiples streams**: viable pero innecesario para 3 consumidores.

## Criterios de aceptación

- [ ] Mismo `event_id` verificable en S3 raw, tabla Iceberg y MV Redshift
- [ ] `IteratorAgeMilliseconds` < retención para cada consumidor
- [ ] Reconciliación por conteo de `event_id` con margen documentado
