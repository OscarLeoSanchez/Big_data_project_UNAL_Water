# Usa la imagen base de PostgreSQL
FROM postgres:latest

# Configura variables de entorno para PostgreSQL
ENV POSTGRES_DB=unalwater
ENV POSTGRES_USER=user
ENV POSTGRES_PASSWORD=root

# Actualiza el sistema y instala `vim`
RUN apt-get update && apt-get install -y vim

# Copia los scripts de configuración al contenedor
COPY setup.sh /docker-entrypoint-initdb.d/setup.sh

# Asegúrate de que los scripts tengan permisos de ejecución
RUN chmod +x /docker-entrypoint-initdb.d/setup.sh

# Expone el puerto 5432
EXPOSE 5432
