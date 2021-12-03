import pyspark.sql.functions as f
import os
from pyspark.sql import SparkSession, WindowSpec, Window, DataFrame, Column

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.common.naming.PlayerInput import *
from minsait.ttaa.datio.common.naming.PlayerOutput import *
from minsait.ttaa.datio.common.naming.ValueCondition import *
from minsait.ttaa.datio.utils.Writer import Writer


class Transformer(Writer):
    def __init__(self, spark: SparkSession):
        self.spark: SparkSession = spark
        df: DataFrame = self.read_input()
        df = self.clean_data(df)
        self.df = df

    def procesar(self):
        df = self.df
        # Ejercicio 5
        df = self.filter_players(df)
        # Ejercicio 1
        df = self.column_selection(df)
        # Ejercicio 2
        df = self.new_column_player_cat(df)
        # Ejercicio 3
        df = self.new_column_potential_vs_overall(df)
        # Ejercicio 4
        df = self.filter_player_cat_and_potential_vs_overall(df)

        # Visualizamos los primeros 5000 registros
        df.show(n=200, truncate=False)

        # Creación de parquet
        self.write(df)

    def read_input(self) -> DataFrame:
        """
        :return: a DataFrame readed from csv file
        """
        return self.spark.read \
            .option(INFER_SCHEMA, True) \
            .option(HEADER, True) \
            .csv(os.getcwd() + INPUT_PATH)

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with filter transformation applied
        column team_position != null && column short_name != null && column overall != null
        """
        df = df.filter(
            (short_name.column().isNotNull()) &
            (short_name.column().isNotNull()) &
            (overall.column().isNotNull()) &
            (team_position.column().isNotNull())
        )
        return df

    def column_selection(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: un DataFrame con las columnas short_name, long_name, age, height_cm, weight_kg, nationality, club_name, overall, potential, team_position
        """
        df = df.select(
            short_name.column(),
            long_name.column(),
            age.column(),
            height_cm.column(),
            weight_kg.column(),
            nationality.column(),
            club_name.column(),
            overall.column(),
            potential.column(),
            team_position.column()
        )
        return df

    def new_column_player_cat(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have team_position and height_cm columns)
        :return: agrega al DataFrame la columna "player_cat"
            A si el jugador es de los mejores 3 jugadores en su posición de su país.
            B si el jugador es de los mejores 5 jugadores en su posición de su país.
            C si el jugador es de los mejores 10 jugadores en su posición de su país.
            D para el resto de jugadores
        """
        w: WindowSpec = Window \
            .partitionBy(nationality.column(), team_position.column()) \
            .orderBy(overall.column().desc())
        rank: Column = f.rank().over(w)

        rule: Column = f.when(rank < 4, CAT_A) \
            .when(rank < 6, CAT_B) \
            .when(rank < 11, CAT_C) \
            .otherwise(CAT_D)

        df = df.withColumn(player_cat.name, rule)

        return df

    def new_column_potential_vs_overall(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have team_position and height_cm columns)
        :return: Agregaremos una columna potential_vs_overall con la siguiente regla:
                 Columna potential dividida por la columna overall
        """
        df = df.withColumn(potential_vs_overall.name, potential.column()/overall.column())
        return df

    def filter_player_cat_and_potential_vs_overall(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have team_position and height_cm columns)
        :return: Filtraremos de acuerdo a las columnas player_cat y potential_vs_overall con las siguientes condiciones:
                    Si player_cat esta en los siguientes valores: A, B
                    Si player_cat es C y potential_vs_overall es superior a 1.15
                    Si player_cat es D y potential_vs_overall es superior a 1.25

        """

        df = df.filter((player_cat.column().isin(CAT_AB) == True)
                       | ((player_cat.column() == CAT_C) & (potential_vs_overall.column() > 1.15))
                       | ((player_cat.column() == CAT_D) & (potential_vs_overall.column() > 1.25)))
        return df

    def filter_players(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have team_position and height_cm columns)
        :return: Si la condición de filtrado de jugadores = 1 realice todos los pasos únicamente para los jugadores menores de 23 años
        """
        if CONDITION_FILTER_PLAYERS == 1:
            df = df.filter((age.column() < 23))
        return df
