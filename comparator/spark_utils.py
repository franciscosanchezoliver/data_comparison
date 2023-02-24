from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql.types import *

class PySparkTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]")\
                        .appName("PySpark for unit testing")\
                        .getOrCreate()

        # Creation of data for the tests
        data_a =\
               [(1, "buy", "10", 0.0123, 123, 2, ("Madrid", "Spain")),
                (2, "buy", "1", 2.0839, 92, 3,  ("Madrid", "Spain")),
                (3, "buy", "1", 23.8320, 50, 1, ("Santiago", "Spain")),
                (4, "Lease", "3", 98.4201, 45, 1, ("Santiago", "Spain")),
                (5, "Lease", "1", 74.0123, 79, 2, ("Santiago", "Spain")),
                (6, "Lease", None, 13.9448, 200, 5, ("Santiago", "Spain")),
                (7, "Lease", "1", 84.0101, 120, 4, ("Santiago", "Spain")),
                (8, "Lease", "5", 0.0001, 100, 3, ("Santiago", "Spain")),
                (9, "Lease", "9", 1.9393, 60, 2, ("Santiago", "Spain")),
                (10, None, "9", 34.8512, 80, 3, (None, "Spain"))]

        data_b = \
            [(5, "Lease", "2", 50.0123, "dalamb@sbcglobal.net", "2", ("Santiago", "Spain")),
             (6, "Lease", "1", 13.9448, "hikoza@mac.com", "3", ("Santiago", "Spain")),
             (7, "Lease", "1", 84.0101, "smartfart@aol.com", "4", ("Santiago", "Spain")),
             (8, "Lease", "3", 2.1293, "cumarana@aol.com", "1", ("Santiago", "Spain")),
             (11, "Lease", "5", 0.0001, "rbarreira@outlook.com", "3", ("Santiago", "Spain")),
             (12, "Lease", "5", 0.0001, "roesch@gmail.com", "1", ("Santiago", "Spain")),
             (12, "buy", "9", 83.9393, "benits@sbcglobal.net" , "2", ("Santiago", "Spain"))
             ]

        data_a_columns = StructType([
            StructField('identifier', IntegerType(), True),
            StructField('operation', StringType(), True),
            StructField('periodicity', StringType(), True),
            StructField('proba_selling', DoubleType(), True),
            StructField('surface', IntegerType(), True),
            StructField('n_rooms', IntegerType(), True),
            StructField('address', StructType([
                StructField('city', StringType(), True),
                StructField('country', StringType(), True)
            ]))
        ])
        data_b_columns = StructType([
            StructField('identifier', IntegerType(), True),
            StructField('operation', StringType(), True),
            StructField('periodicity', StringType(), True),
            StructField('proba_selling', DoubleType(), True),
            StructField('contact_email', StringType(), True),
            StructField('n_rooms', StringType(), True),
            StructField('address', StructType([
                StructField('city', StringType(), True),
                StructField('country', StringType(), True)
            ]))
        ])

        cls.df_a = cls.spark.createDataFrame(data=data_a, schema=data_a_columns)
        cls.df_b = cls.spark.createDataFrame(data=data_b, schema=data_b_columns)
        cls.df_a.createOrReplaceTempView("df_a")
        cls.df_b.createOrReplaceTempView("df_b")


    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
