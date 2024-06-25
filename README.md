## Intrucciones

1. Clonar el repositorio

   ```bash
       git clone https://github.com/OscarLeoSanchez/UNALWater_BigData_Project.git
   ```

2. Crear un entorno virtual

   ```bash
       python -m venv .venv
   ```

3. Activar el entorno virtual

   **linux**

   ```bash
       source env/bin/activate
   ```

   **windows**

   ```bash
       env\Scripts\activate
   ```

4. Instalar las dependencias

```bash
    pip install -r requirements.txt
```

### Ejecución

1. Ejecutar el server

```bash
    python server/server.py --[OPTIONS]
```

Opciones:

- --host: Host del servidor
- --port: Puerto del servidor
- --interval: Intervalo de tiempo en segundos para enviar datos al cliente (default=30)
- --batch_size: Tamaño del batch de datos a enviar al cliente (default=500) `batch_size=-1` genera un batch de tamaño aleatorio entre 1 y 500
- --initial_date: Fecha inicial para la generación de datos `YYYY-MM-DD` (default=2010-01-01)

2. Ejecutar el cliente

```bash
    python client/consumer.py
```

### Contenedores de docker

1. **Crear la imagen**
   Ingresar a la carpeta `server` y ejecutar el siguiente comando:

   ```bash
       docker build -t unalwater_server .
   ```

2. **Crear la red**

   ```bash
       docker network create unalwater_network
   ```

3. **Contenedor server**

- _Ejecución del con parámetros por defecto_

  ```bash
      docker run -d --name unalwater_server --network unalwater_network -v ${pwd}:/app unalwater_server
  ```

      docker run -d --name unalwater_server --network unalwater_network -v ${pwd}:/app unalwater_server
      docker run -d --name unalwater_db -e POSTGRES_PASSWORD=root -e POSTGRES_USER=user -e POSTGRES_DB=unalwater --network unalwater_network -p 5431:5432 unalwater_db
      docker run --rm -it --name spark -p 4040:4040 -p 5007:50070 -p 8088:8088 -p 8888:8888 --network unalwater_network -v ${PWD}:/workspace jdvelasq/spark:3.1.3

- _Ejecución con parámetros personalizados_

  ```bash
      docker run -d --name unalwater_server --network unalwater_network -v ${pwd}:/app unalwater_server --interval 30 --batch_size 10 --initial_date "2021-01-01"
  ```

4. **Contenedor de spark**

```bash
    docker run --rm -it --name spark -p 4040:4040 -p 5007:50070 -p 8088:8088 -p 8888:8888 --network unalwater_network -v ${PWD}:/workspace jdvelasq/spark:3.1.3
```

5. **Contenedor db Postgres**

```bash
    docker run -d --name unalwater_db -p 5432:5432 --network unalwater_network -v ${pwd}:/var/lib/postgresql/data unalwater_db
    docker run -d --name unalwater_db -e POSTGRES_PASSWORD=root -e POSTGRES_USER=user -e POSTGRES_DB=unalwater --network unalwater_network -p 5431:5432 unalwater_db
```

6. **Contenedor MariaDB**

```bash
    docker run -d --name unalwater_db -e MYSQL_ROOT_PASSWORD=Una1wat3R -e MYSQL_DATABASE=unalwater --network unalwater_network -p 3306:3306 mariadb
    docker run --rm -it --name mariadb -p 5008:50070 -p 8089:8088 -p 8889:8888 --network unalwater_network -v ${PWD}:/workspace jdvelasq/mariadb:10.3.34
```

#### Ejecución de Spark _(En desarrollo....)_

1. Ingresar al contenedor de spark

```bash
    docker exec -it spark bash
```

2. Ejecutar el script de bronze

```bash
    spark-submit spark_jobs/process_bronze.py
```

3. Ejecutar el script de silver

```bash
    spark-submit spark_jobs/process_silver.py
```

4. Ejecutar el script de gold

```bash
    spark-submit spark_jobs/process_gold.py
```

#### **PRUEBAS**

Dado que se está utilizando un contenedor de prueba, se debe instalar las dependiencias necesarias para ejecutar los scripts de spark.

```bash
    pip install -r requirements.txt
```

El archivo `spark_jobs/process_bronze.py` se encarga de leer los datos generados por el servidor y guardarlos en formato parquet en la carpeta `bronze`.

El archivo `spark_jobs/process_silver.py` se encarga de leer los datos de la carpeta `bronze` y guardarlos en formato parquet en la carpeta `silver`.

El archivo `spark_jobs/process_gold.py` se encarga de leer los datos de la carpeta `silver` y guardarlos en formato parquet en la carpeta `gold`.

**NOTA:** Los scripts de spark se encuentran en desarrollo, por lo que no se garantiza su correcto funcionamiento.

**NOTA:** El archivo `spark_pb.py` se creo para ejecutar pruebas

### Instalaciones en el contenedor de spark

wget https://jdbc.postgresql.org/download/postgresql-42.5.0.jar

curl -O https://jdbc.postgresql.org/download/postgresql-42.5.0.jar

mv postgresql-42.5.0.jar /opt/spark/jars/

echo "spark.driver.extraClassPath /opt/spark/jars/postgresql-42.5.0.jar" >> $SPARK_HOME/conf/spark-defaults.conf

spark-submit --jars /opt/spark/jars/postgresql-42.5.0.jar process_gold.py

apt-get update

docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' unalwater_db
