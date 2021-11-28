import pyspark.sql.functions as f
import os
from pyspark.sql import SparkSession, WindowSpec, Window, DataFrame, Column

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.common.naming.PlayerInput import *
from minsait.ttaa.datio.common.naming.PlayerOutput import *
from minsait.ttaa.datio.common.naming.Valuecondition import *
from minsait.ttaa.datio.utils.Writer import Writer


class Transformer(Writer):
    def __init__(self, spark: SparkSession):
        self.spark: SparkSession = spark
        df: DataFrame = self.read_input()
        df.printSchema()
        df = self.clean_data(df)
#        df = self.example_window_function(df)
#       Código antiguo{
#        df = self.column_selection(df)
#       }Código antiguo
#       Código nuevo{
        df = self.filter_players(df, 1)
        df = self.column_selection_prueba(df)
        df = self.new_column_player_cat(df)
        df = self.new_column_potential_vs_overall(df)
        df = self.filter_player_cat_and_potential_vs_overall(df)
        self.write_with_partitionby(df)
#       }Código nuevo

        # for show 100 records after your transformations and show the DataFrame schema
        df.show(n=100, truncate=False)
        df.printSchema()

        # Uncomment when you want write your final output
        self.write(df)

    def read_input(self) -> DataFrame:
        """
        :return: a DataFrame readed from csv file
        """
        return self.spark.read \
            .option(INFER_SCHEMA, True) \
            .option(HEADER, True) \
            .csv(INPUT_PATH)

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
        :return: a DataFrame with just 5 columns...
        """
        df = df.select(
            short_name.column(),
            overall.column(),
            height_cm.column(),
            team_position.column(),
            catHeightByPosition.column()
        )
        return df

    def example_window_function(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have team_position and height_cm columns)
        :return: add to the DataFrame the column "cat_height_by_position"
             by each position value
             cat A for if is in 20 players tallest
             cat B for if is in 50 players tallest
             cat C for the rest
        """
        w: WindowSpec = Window \
            .partitionBy(team_position.column()) \
            .orderBy(height_cm.column().desc())
        rank: Column = f.rank().over(w)

        rule: Column = f.when(rank < 10, "A") \
            .when(rank < 50, "B") \
            .otherwise("C")

        df = df.withColumn(player_cat.name, rule)
        return df

    def column_selection_prueba(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with just 5 columns...
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
    #        catHeightByPosition.column()
        )
        return df

    def new_column_player_cat(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have team_position and height_cm columns)
        :return: add to the DataFrame the column "cat_height_by_position"
             by each position value
             cat A for if is in 20 players tallest
             cat B for if is in 50 players tallest
             cat C for the rest
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
        :return: add to the DataFrame the column "cat_height_by_position"
             by each position value
             cat A for if is in 20 players tallest
             cat B for if is in 50 players tallest
             cat C for the rest
        """
        df = df.withColumn(potential_vs_overall.name, potential.column()/overall.column())
        return df

    def filter_player_cat_and_potential_vs_overall(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have team_position and height_cm columns)
        :return: add to the DataFrame the column "cat_height_by_position"
             by each position value
             cat A for if is in 20 players tallest
             cat B for if is in 50 players tallest
             cat C for the rest
        """
        li = ["A", "B"]
        df = df.filter((player_cat.column().isin(li) == True)
                       | ((player_cat.column() == "C") & (potential_vs_overall.column() > 1.15))
                       | ((player_cat.column() == "D") & (potential_vs_overall.column() > 1.25)))
        return df

    def filter_players(self, df: DataFrame, condition: int) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have team_position and height_cm columns)
        :return: add to the DataFrame the column "cat_height_by_position"
             by each position value
             cat A for if is in 20 players tallest
             cat B for if is in 50 players tallest
             cat C for the rest
        """
        if condition == 1:
            df = df.filter((age.column() < 23))
        return df
