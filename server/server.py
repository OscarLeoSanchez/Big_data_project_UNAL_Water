import socket
import time
import json
import random
import argparse
from faker import Faker
from shapely.geometry import Point
import osmnx as ox
import geopandas as gpd
import pandas as pd
from shapely import wkb
from common.utils import configure_log
import os
import struct
import zlib

# project_root = os.path.dirname(__file__)
# os.chdir(project_root)


fake = Faker()

logger = configure_log("server", "server.log")


class PolygonFinder:
    def __init__(self, parquet_file):
        self.gdf = self.load_parquet_file(parquet_file)

    def load_parquet_file(self, parquet_file):
        df = pd.read_parquet(parquet_file)
        df["geometry"] = df["geometry"].apply(self._wkb_to_wkt)
        return gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df["geometry"]))

    def get_neighbord_from_coordinates(self, lat, lon):
        point = Point(lon, lat)
        matching_polygon = self.gdf[self.gdf.contains(point)]
        if not matching_polygon.empty:
            polygon_id = matching_polygon.iloc[0]["OBJECTID"]
            return polygon_id
        else:
            return None

    def generate_random_points_at_poligon(self):
        polygon = self.gdf.geometry.iloc[0]
        minx, miny, maxx, maxy = polygon.bounds
        while True:
            point = Point(random.uniform(minx, maxx), random.uniform(miny, maxy))
            if polygon.contains(point):
                return point.y, point.x

    def _wkb_to_wkt(self, wkb_data):
        return wkb.loads(wkb_data).wkt


class ValueRangeFinder:
    def __init__(self, csv_file):
        self.df = self.load_csv_file(csv_file)

    def load_csv_file(self, csv_file):
        df = pd.read_csv(csv_file, sep=";")
        return df

    def get_value_range(self, polygon_id):
        row = self.df[self.df["objec_id"] == polygon_id]
        if not row.empty:
            min_value = row.iloc[0]["t_min"]
            max_value = row.iloc[0]["t_max"]
            return min_value, max_value
        else:
            return None, None


class ColumnData:
    def __init__(self, parquet_file, column_name):
        self.df = self.load_parquet_file(parquet_file)
        self.column_name = column_name

    def load_parquet_file(self, parquet_file):
        df = pd.read_parquet(parquet_file)
        return df

    def get_column_data(self):
        return self.df[self.column_name].tolist()


path_medellin_poligon = "data/50001.parquet"
path_neighbors = "data/medellin_neighborhoods.parquet"
path_value_range = "data/range_time.csv"
path_customers = "data/customers.parquet"
path_employees = "data/employees.parquet"

polygon_medellin = PolygonFinder(path_medellin_poligon)
polygon_finder = PolygonFinder(path_neighbors)
value_range_finder = ValueRangeFinder(path_value_range)
customers_data = ColumnData(path_customers, "customer_id")
employees_data = ColumnData(path_employees, "employee_id")
id_customers = customers_data.get_column_data()
id_employees = employees_data.get_column_data()


def generate_random_points_at_medellin():
    return polygon_medellin.generate_random_points_at_poligon()
    # place_name = "Medellín, Colombia"
    # df_medellin = ox.geocode_to_gdf(place_name)
    # polygon = df_medellin.geometry.iloc[0]

    # minx, miny, maxx, maxy = polygon.bounds
    # while True:
    #     point = Point(random.uniform(minx, maxx), random.uniform(miny, maxy))
    #     if polygon.contains(point):
    #         return point.y, point.x


def get_random_customer_id():
    return random.choice(id_customers)


def get_random_employee_id():
    return random.choice(id_employees)


def get_dispatch_date(order_date):
    order_date = pd.to_datetime(order_date)
    dispatch_date = order_date + pd.Timedelta(minutes=random.randint(2, 60))
    if dispatch_date.hour >= 18:
        dispatch_date = order_date + pd.Timedelta(days=1)
        minute = random.randint(1, 20)
        dispatch_date = dispatch_date.replace(hour=7, minute=minute, second=0)
    return dispatch_date.isoformat()


def get_delivery_date(dispatch_date, latitude, longitude):
    id_neighbord = polygon_finder.get_neighbord_from_coordinates(latitude, longitude)
    if id_neighbord is not None:
        t_min, t_max = value_range_finder.get_value_range(id_neighbord)
        if t_min and t_max:
            minutes = random.randint(t_min, t_max)
            dispatch_date = pd.to_datetime(dispatch_date)
            deliver_date = dispatch_date + pd.Timedelta(minutes=minutes)
            return deliver_date.isoformat()


def generate_date(initial_date, max_time=60):
    initial_date = pd.to_datetime(initial_date)
    if initial_date.hour < 7:
        initial_date = initial_date.replace(hour=7, minute=0, second=0)
    elif initial_date.hour >= 18:
        initial_date = initial_date + pd.Timedelta(days=1)
        initial_date = initial_date.replace(hour=7, minute=0, second=0)
    date = initial_date + pd.Timedelta(minutes=random.randint(1, max_time))
    return date.isoformat()


def generate_data(initial_date):
    flat = True
    while flat:
        latitude, longitude = generate_random_points_at_medellin()
        order_date = generate_date(initial_date, 90)
        dispatch_date = get_dispatch_date(order_date)
        delivery_date = get_delivery_date(dispatch_date, latitude, longitude)
        if delivery_date:
            flat = False
        else:
            continue
        data = {
            "latitude": latitude,
            "longitude": longitude,
            "order_date": order_date,
            "dispatch_date": dispatch_date,
            "delivery_date": delivery_date,
            "customer_id": get_random_customer_id(),
            "employee_id": get_random_employee_id(),
            "quantity_products": random.randint(1, 100),
            "order_id": fake.uuid4(),
        }
    return data


def save_parquet_file(data, file_name):
    df = pd.DataFrame(data)
    df.to_parquet(file_name, index=False)


def generate_batch_data(initial_date: str, batch_size=1000):
    batch_data = []
    generated_date = {}
    for _ in range(batch_size):
        order_date = (
            generated_date.get("order_date") if generated_date else initial_date
        )

        generated_date = generate_data(order_date)
        batch_data.append(generated_date)
    return batch_data, generated_date["order_date"]


def save_list_dict(list_dict, file_name):
    dir_name = os.path.dirname(file_name)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    df = pd.DataFrame(list_dict)
    df.to_csv(file_name, index=False)


def send_data(conn, data):
    data_json = json.dumps(data).encode("utf-8")
    data_length = len(data_json)
    checksum = zlib.crc32(data_json)
    header = struct.pack("!I I", data_length, checksum)

    try:
        conn.sendall(header)
        conn.sendall(data_json)
    except Exception as e:
        logger.error(f"Error sending data: {e}")
        return False
    return True


def send_parquet_file(conn, batch_data, batch_size, count_data):
    file_name = "output/data.parquet"
    save_list_dict(batch_data, file_name)

    logger.info(f"Sending {batch_size} data")

    with open(file_name, "rb") as f:
        data = f.read()
        try:
            conn.sendall(data)
            count_data += batch_size
            logger.info(f"File sent with {count_data} registers")
        except Exception as e:
            logger.error(f"Error sending data: {e}")
            logger.error(f"Error data: {file_name}")
            logger.info("Closing connection")
            logger.info(f"Total data sent: {count_data}")
            exit(1)
        return count_data


def send_list_json(conn, batch_data, count_data):
    data_json = json.dumps(batch_data) + "\n"
    conn.sendall(data_json.encode("utf-8"))
    count_data = len(batch_data)
    return count_data


def send_json(conn, batch_data, count_data):
    for data in batch_data:
        try:
            conn.sendall((json.dumps(data) + "\n").encode("utf-8"))
            count_data += 1
        except Exception as e:
            logger.error(f"Error sending data: {e}")
            logger.error(f"Error data: {data}")
            logger.info("Closing connection")
            logger.info(f"Total data sent: {count_data}")
            exit(1)
        time.sleep(0.001)
    return count_data


def start_server(
    host="0.0.0.0", port=65432, interval=30, batch_size=1000, initial_date="2010-01-01"
):
    logger.info(f"Starting server at {host}:{port}")
    is_random = False
    if batch_size == -1:
        is_random = True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()
        logger.info(f"Server started at {host}:{port}")
        while True:
            conn, addr = s.accept()
            with conn:
                logger.info(f"Connected by {addr}")
                count_data = 0
                while True:
                    try:
                        if is_random:
                            batch_size = random.randint(1, 1000)

                        batch_data, last_order_date = generate_batch_data(
                            initial_date=initial_date, batch_size=batch_size
                        )

                        count_data = send_json(conn, batch_data, count_data)
                        # count_data = send_parquet_file(conn, batch_data, batch_size, count_data)
                        # count_data = send_list_json(conn, batch_data, count_data)
                        logger.info(f"Data sent: {count_data}")
                        time.sleep(interval)
                        initial_date = last_order_date
                    except KeyboardInterrupt:
                        logger.info("Server stopped by user")
                        logger.info(f"Total data sent: {count_data}")
                        return
                    except Exception as e:
                        logger.error(f"Unexpected error: {e}")
                        logger.info("Closing connection")
                        logger.info(f"Total data sent: {count_data}")
                        break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Generation Server")
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Intervalo de generación de datos en segundos",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=5,
        help="Número de datos a enviar en cada lote",
    )
    parser.add_argument(
        "--initial_date",
        type=str,
        default="2010-01-01",
        help="Fecha inicial para la generación de datos (formato YYYY-MM-DD)",
    )

    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host para el servidor",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=65432,
        help="Puerto para el servidor",
    )

    args = parser.parse_args()

    start_server(
        host=args.host,
        port=args.port,
        interval=args.interval,
        batch_size=args.batch_size,
        initial_date=args.initial_date,
    )


# py .\data_generator\generator.py --interval=5 --batch_size=-1 --initial_date=2000-03-04T17:04:00
