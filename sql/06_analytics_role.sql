-- ============================================================
-- PE6 — Paso 6: Rol de analítica con acceso restringido
--
-- Cumple el requisito de seguridad del enunciado:
-- "Asegura que el acceso al esquema esté restringido a un rol
--  específico de analítica creado por ti."
--
-- Separa el acceso de consulta (analítica) del usuario
-- administrador. El rol solo puede LEER los esquemas de la
-- solución; no puede crear, modificar ni borrar objetos.
--
-- BLOQUEADO_POR_COSTO: requiere Redshift activo.
-- Estrategia declarativa; en el lab se usó LabRole (admin).
-- ============================================================

-- ---------------------------------------------------------
-- 1. Crear el rol de analítica
-- ---------------------------------------------------------
CREATE ROLE analytics_reader;

-- ---------------------------------------------------------
-- 2. Permiso de USO solo sobre los esquemas de la solución
--    (sin acceso al resto de la base de datos)
-- ---------------------------------------------------------
GRANT USAGE ON SCHEMA kinesis_streaming TO ROLE analytics_reader;
GRANT USAGE ON SCHEMA lakehouse         TO ROLE analytics_reader;

-- ---------------------------------------------------------
-- 3. Solo lectura (SELECT) sobre la MV y las tablas Iceberg
--    Sin DDL, sin escritura, sin DROP
-- ---------------------------------------------------------
GRANT SELECT ON mv_kinesis_transactions         TO ROLE analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse  TO ROLE analytics_reader;

-- Opcional: aplicar también a tablas futuras del esquema lakehouse
ALTER DEFAULT PRIVILEGES IN SCHEMA lakehouse
    GRANT SELECT ON TABLES TO ROLE analytics_reader;

-- ---------------------------------------------------------
-- 4. Usuario de analítica asignado al rol
--    (contraseña gestionada fuera del SQL — Secrets Manager)
-- ---------------------------------------------------------
CREATE USER analyst_user PASSWORD DISABLE;
GRANT ROLE analytics_reader TO analyst_user;

-- ---------------------------------------------------------
-- 5. Verificación del rol y sus permisos
-- ---------------------------------------------------------
-- Roles existentes
SELECT role_name FROM svv_roles WHERE role_name = 'analytics_reader';

-- Permisos otorgados al rol
SELECT
    namespace_name,
    object_name,
    privilege_type
FROM svv_role_grants
WHERE role_name = 'analytics_reader';
