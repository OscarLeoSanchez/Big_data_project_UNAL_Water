import psycopg2


def create_table():
    try:
        connection = psycopg2.connect(
            user="user",
            password="root",
            host="unalwater_db",
            port="5432",
            database="unalwater",
        )
        cursor = connection.cursor()

        # Verificar si la tabla 'daily_sales' existe
        cursor.execute(
            """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'daily_sales'
        );
        """
        )
        exists = cursor.fetchone()[0]

        if not exists:
            create_table_query = """
            CREATE TABLE daily_sales (
                order_date DATE NOT NULL,
                neighborhood VARCHAR(255) NOT NULL,
                comune VARCHAR(100) NOT NULL,
                working_day VARCHAR(2) NOT NULL,
                total_quantity INT NOT NULL,
                order_count INT NOT NULL,
                average_delivery_time NUMERIC(5, 2) NOT NULL,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL
            );
            """
            cursor.execute(create_table_query)
            connection.commit()
            print("Tabla 'daily_sales' creada con éxito.")
        else:
            print("La tabla 'daily_sales' ya existe.")

    except (Exception, psycopg2.Error) as error:
        print("Error al conectar a PostgreSQL o al ejecutar la consulta", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Conexión a PostgreSQL cerrada.")
