#!/bin/bash
set -e

# Variables de configuración
PG_HBA_CONF="/var/lib/postgresql/data/pg_hba.conf"
PG_CONF="/var/lib/postgresql/data/postgresql.conf"

# Esperar a que la base de datos esté inicializada
until pg_isready -q; do
    echo "Waiting for database to start..."
    sleep 2
done

# Crear el rol 'postgres' si no existe
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'postgres') THEN
            CREATE ROLE postgres WITH LOGIN SUPERUSER PASSWORD 'root';
        END IF;
    END
    \$\$;
EOSQL

# Modificar pg_hba.conf para permitir conexiones externas
echo "host    all             all             0.0.0.0/0               md5" >> "$PG_HBA_CONF"

# Modificar postgresql.conf para escuchar en todas las interfaces
sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/g" "$PG_CONF"

echo "Database configurations have been set."
