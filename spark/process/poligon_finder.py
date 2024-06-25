from pyspark.sql import SparkSession
from shapely.geometry import Point
import geopandas as gpd
import pandas as pd
from geopandas import GeoDataFrame
from shapely import wkb


class PolygonFinder:
    def __init__(self, parquet_file):
        # self.spark = SparkSession.builder.appName("PolygonFinder").getOrCreate()
        self.gdf = self.load_parquet_file(parquet_file)

    def load_parquet_file(self, parquet_file):
        # df_spark = self.spark.read.parquet(parquet_file)
        # df_spark.printSchema()
        # df_spark.show(truncate=True)
        # polygon_df = df_spark.select("geometry").collect()
        df = pd.read_parquet(parquet_file)
        df["geometry"] = df["geometry"].apply(self._wkb_to_wkt)
        gdf = GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df["geometry"]))
        return gdf

    def get_neighbord_from_coordinates(self, lat, lon):
        point = Point(lon, lat)
        matching_polygon = self.gdf[self.gdf.contains(point)]
        if not matching_polygon.empty:
            neighborhood = matching_polygon.iloc[0]["NOMBRE"]
            comune = matching_polygon.iloc[0]["IDENTIFICACION"]
            return neighborhood, comune
        else:
            return None

    def close_spark_session(self):
        self.spark.stop()

    def _wkb_to_wkt(self, wkb_data):
        return wkb.loads(wkb_data).wkt
