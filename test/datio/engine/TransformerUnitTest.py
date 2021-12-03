from minsait.ttaa.datio.engine.Transformer import Transformer
from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.common.naming.PlayerInput import *
from minsait.ttaa.datio.common.naming.PlayerOutput import *
from minsait.ttaa.datio.common.naming.ValueCondition import *
from pyspark.sql import SparkSession

import unittest
import os

spark: SparkSession = SparkSession \
            .builder \
            .master(SPARK_MODE) \
            .getOrCreate()
#Seteamos la ruta en la carpeta principal, dado a que estamos en otra ubicación
os.chdir('../../../')
#Creamos una instancia del objeto para realizar con ella las pruebas
transformer = Transformer(spark)

class TransformerUnitTest(unittest.TestCase):

    def test_ejercicio1(self):

        print("Test Unitario del Ejercicio 1")
        transformer.df = transformer.df
        transformer.df = transformer.column_selection(transformer.df)

        #Buscamos que el resultado final tenga las mismas columnas indicadas en el ejercicio 1
        cols = transformer.df.columns #Obtenemos las columnas del dataframe

        self.assertTrue(short_name.name in cols, "Error, falta la columna short_name")
        self.assertTrue(long_name.name in cols, "Error, falta la columna long_name")
        self.assertTrue(age.name in cols, "Error, falta la columna age")
        self.assertTrue(height_cm.name in cols, "Error, falta la columna height_cm")
        self.assertTrue(weight_kg.name in cols, "Error, falta la columna weight_kg")
        self.assertTrue(nationality.name in cols, "Error, falta la columna nationality")
        self.assertTrue(club_name.name in cols, "Error, falta la columna club_name")
        self.assertTrue(overall.name in cols, "Error, falta la columna overall")
        self.assertTrue(potential.name in cols, "Error, falta la columna potential")
        self.assertTrue(team_position.name in cols, "Error, falta la columna team_position")

        #Verificamos que el número de columnas obtenidas sean solo las indicadas 10
        self.assertTrue(len(cols) == 10, "Error, el número de columnas resultantes es diferente a lo esperado")

    def test_ejercicio2(self):
        print("\nTest Unitario del Ejercicio 2")
        transformer.df = transformer.new_column_player_cat(transformer.df)
        #Verificamos que el dataframe resultante tenga la nueva columna creada, sino hubo un error
        self.assertTrue(player_cat.name in transformer.df.columns, "Error, falta la columna player_cat")
        #Averiguar como se validaría que la selección con Window está bien

    def test_ejercicio3(self):
        print("\nTest Unitario del Ejercicio 3")
        transformer.df = transformer.new_column_potential_vs_overall(transformer.df)
        #Verificamos que el dataframe resultante tenga la nueva columna creada, sino hubo un error
        self.assertTrue(potential_vs_overall.name in transformer.df.columns, "Error, falta la columna potential_vs_overall")
        #Seleccionamos el 10% registros al azar
        data_itr = transformer.df.sample(0.001).rdd.toLocalIterator()
        # Recorremos los registros al azar seleccionados
        for row in data_itr:
            #Verificamos que el resultado sea el correcto
            self.assertTrue(row[potential.name] / row[overall.name] == row[potential_vs_overall.name], "Error, falta la columna potential_vs_overall")

    def test_ejercicio4(self):
        print("\nTest Unitario del Ejercicio 4")
        transformer.df = transformer.filter_player_cat_and_potential_vs_overall(transformer.df)
        #De los registros ya filtrados, verificamos que no exista el complemento de la condición del filtro
        df = transformer.df
        df = df.filter(((player_cat.column() == CAT_C) & (potential_vs_overall.column() <= 1.15))
                       | ((player_cat.column() == CAT_D) & (potential_vs_overall.column() <= 1.25)))
        #Verificamos que el dataframe no contenga elementos para filtro complemento
        self.assertTrue(df.count() == 0, "Error, no se ha realizado correctamente el filtrado")

    def test_ejercicio5(self):
        print("\nTest Unitario del Ejercicio 5")
        #Solo si la condición es 1
        if CONDITION_FILTER_PLAYERS == 1:
            transformer.df = transformer.filter_players(transformer.df)
            df = transformer.df
            df = df.filter((age.column() >= 23))
            #Verificamos que el dataframe no contenga elementos para filtro complemento
            self.assertTrue(df.count() == 0, "Error, no se ha realizado correctamente el filtrado")
