
from minsait.ttaa.datio.engine.Transformer import Transformer
import unittest

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from minsait.ttaa.datio.common.Constants import *
from pyspark.sql import SparkSession

class TransformerUnitTest(unittest.TestCase):

    def test_ejercicio1(self):
        spark: SparkSession = SparkSession \
            .builder \
            .master(SPARK_MODE) \
            .getOrCreate()
        print("Test Unitario del Ejercicio 1")
        data = [("L. Messi", "Lionel AndrÃ©s Messi Cuccittini", 33, 170, 72, "Argentina", "FC Barcelona", 93, 93, "CAM"),
                 ("Cristiano Ronaldo", "Cristiano Ronaldo dos Santos Aveiro", 35, 187, 83, "Portugal", "Juventus", 92, 92, "LS"),
                 ("J. Oblak", "Jan Oblak", 33, 188, 87, "Slovenia", "AtlÃ©tico Madrid", 91, 93, "GK"),
                 ("R. Lewandowski", "Robert Lewandowski", 31, 184, 80, "Poland", "FC Bayern MÃ¼nchen", 91, 91, "ST"),
                 ("Neymar Jr", "Neymar da Silva Santos JÃºnior", 28, 175, 68, "Brazil", "Paris Saint-Germain", 91, 91, "LW")
                 ]

        schema = StructType([ \
            StructField("short_name", StringType(), True), \
            StructField("long_name", StringType(), True), \
            StructField("age", IntegerType(), True), \
            StructField("height_cm", IntegerType(), True), \
            StructField("weight_kg", IntegerType(), True), \
            StructField("nationality", StringType(), True), \
            StructField("club_name", StringType(), True),\
            StructField("overall", IntegerType(), True), \
            StructField("potential", IntegerType(), True), \
            StructField("team_position", StringType(), True)
            ])
        df = spark.createDataFrame(data=data, schema=schema)
        df2 = df
        df2 = Transformer(spark).clean_data(df2)
        # Ejercicio 5
        df2 = Transformer(spark).filter_players(df2)

        print("Se terminó la prueba")
        self.assertEquals(Transformer(spark).column_selection(df2), df)
