from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.sql import SparkSession

class Comparator:

    def __init__(self, df_a, df_b,
                 common_key,
                 tag_for_a="_a",
                 tag_for_b="_b",
                 columns_to_compare=[],
                 examples_number_in_txt_report=5):
        self.df_a = df_a
        self.df_b = df_b
        self.common_key = common_key
        self.tag_for_a = tag_for_a
        self.tag_for_b = tag_for_b
        self.columns_to_compare = columns_to_compare
        self.examples_number_in_txt_report = examples_number_in_txt_report

    def compare(self):
        self.get_common_columns()
        self.rename_columns()
        self.discard_columns_with_different_types()
        self.get_commons()


    def rename_columns(self):
        """
        to be able to compare the tables we need to rename the columns
        to know which column belong to which table

        """
        self.df_a = self.df_a.select([f.col(col).alias(col + '_' + self.tag_for_a) for col in self.df_a.columns])
        self.df_b = self.df_b.select([f.col(col).alias(col + '_' + self.tag_for_b) for col in self.df_b.columns])


    def get_common_columns(self):
        """
        # Get common columns if the user hasn't specified any
        :return:
        """
        if len(self.columns_to_compare) == 0:
            self.columns_to_compare = set(self.df_a.columns).intersection(set(self.df_b.columns))

        # Don't consider the key
        if self.common_key in self.columns_to_compare:
            self.columns_to_compare.remove(self.common_key)

        # from set to list
        self.columns_to_compare = list(self.columns_to_compare)

        # order in alphabetical order
        self.columns_to_compare.sort()

        return self.columns_to_compare

    def get_commons_rows(self):
        """
        get the common rows (inner join)
        """
        self.commons = self.df_a.join(self.df_b,
                                      on=self.df_a[f"{self.common_key}_{self.tag_for_a}"] ==
                                         self.df_b[f"{self.common_key}_{self.tag_for_b}"])

        return self.commons


    def discard_columns_with_different_types(self):
        # Discard columns with different type
        self.columns_to_discard = []
        for col_name in self.columns_to_compare:

            same_type = self.df_a.select(f"{col_name}_{self.tag_for_a}").dtypes[0][1] == \
                        self.df_b.select(f"{col_name}_{self.tag_for_b}").dtypes[0][1]
            if not same_type:
                print(f"Discarting {col_name} because it is not the same type in both dataframes")
                self.columns_to_compare.remove(col_name)
                self.columns_to_discard.append(col_name)

    def check_equality_between_columns(self):
        fields_to_select = []
        for field in self.columns_to_compare:
            col_name_a = f"{field}_{self.tag_for_a}"
            col_name_b = f"{field}_{self.tag_for_b}"
            fields_to_select.append(col_name_a)
            fields_to_select.append(col_name_b)
            fields_to_select.append(
                ((f.col(col_name_a) == f.col(col_name_b)) |
                 ((f.col(col_name_a).isNull()) & (f.col(col_name_b).isNull()))
                 ).alias(field + "_equals"))

        self.commons = self.commons.select(
            [f"{self.common_key}_{self.tag_for_a}",
             f"{self.common_key}_{self.tag_for_b}"] + fields_to_select)

    def calculate_stats_of_comparison(self):
        self.stats = {}
        for col in [col for col in self.commons.columns if "equal" in col.lower()]:
            results = self.commons.select(col).groupBy(col).count().collect()

            counts_for_this_col = {}

            total = 0
            for res in results:
                total += res[1]

            for res in results:
                val = res[0]
                count = res[1]
                if val is None:
                    val = "Null"

                counts_for_this_col[val] = count
                counts_for_this_col[str(val) + "_percentage"] = count / total

            self.stats[col] = counts_for_this_col

        return self.stats

    def create_spark_df_with_stats(self):
        compare_table_structure = StructType([
            StructField('column', StringType(), True),
            StructField('equals', IntegerType(), True),
            StructField('equals_percentage', DoubleType(), True),
            StructField('different', IntegerType(), True),
            StructField('different_percentage', DoubleType(), True),
            StructField('empty', IntegerType(), True),
            StructField('empty_percentage', DoubleType(), True),
        ])

        # Convert the dict of stats in a SparkDataframe
        data = []
        for col, value in self.stats.items():
            new_row = (
                # Name of the column
                col,
                # Number of same values
                value[True] if True in value else 0,
                # Percentage of same values
                value['True_percentage'] if 'True_percentage' in value else 0.0,
                # Number of different values
                value[False] if False in value else 0,
                # Percentage of different values
                value['False_percentage'] if 'False_percentage' in value else 0.0,
                # Number of empty values
                value['Null'] if 'Null' in value else 0,
                # Percentage of empty values
                value['Null_percentage'] if 'Null_percentage' in value else 0.0
            )
            data.append(new_row)

        spark = SparkSession.builder.getOrCreate()
        self.df_comparison = spark.createDataFrame(data=data, schema=compare_table_structure)
        # Order by the values that have the most coincidence rate

        self.df_comparison = self.df_comparison.orderBy(f.desc("equals_percentage"),
                                                        f.desc("different_percentage"),
                                                        f.desc("empty_percentage"))

        return self.df_comparison

    def generate_differences_report_as_string(self):
        results_ranking_with_fails = self.df_comparison.filter(f.col("equals_percentage") < 1).collect()

        self.diffences_report_as_txt = ""
        # Show examples of the differences
        for i, row in enumerate(results_ranking_with_fails):
            column = row['column']
            equals = row['equals']
            equals_percentage = row['equals_percentage']
            different = row['different']
            different_percentage = row['different_percentage']
            empty = row['empty']
            empty_percentage = row['empty_percentage']

            column = column.split("_equals")[0]
            self.diffences_report_as_txt += "{}/{}\n".format(i + 1, len(results_ranking_with_fails))
            self.diffences_report_as_txt += "-" * len(column) + "\n"
            self.diffences_report_as_txt += column + "\n"
            self.diffences_report_as_txt += "-" * len(column) + "\n"
            self.diffences_report_as_txt += "equals {}({})\n".format("{:.4f}%".format(equals_percentage * 100),
                                         equals)
            self.diffences_report_as_txt += "different {}({})\n".format("{:.4f}%".format(different_percentage * 100),
                                            different)
            self.diffences_report_as_txt += "empty {}({})\n".format("{:.4f}%".format(empty_percentage * 100), empty)

            if different > 0:
                self.diffences_report_as_txt += "Differents values\n"
                # Get an example of different values
                example_of_differences = self.commons.select(f"{self.common_key}_{self.tag_for_a}",
                                                             f"{self.common_key}_{self.tag_for_b}",
                                                             f"{column}_equals") \
                                                            .filter(~f.col(f"{column}_equals"))
                self.diffences_report_as_txt += self.getShowString(example_of_differences) + "\n"

            if empty > 0:
                self.diffences_report_as_txt += "Empty values\n"
                example_of_differences = self.commons.select(f"{self.common_key}_{self.tag_for_a}",
                                                             f"{self.common_key}_{self.tag_for_b}",
                                                             f"{column}_equals")\
                    .filter(f.col(column + "_equals").isNull())
                self.diffences_report_as_txt += self.getShowString(example_of_differences) + "\n"

        return self.diffences_report_as_txt



    def getShowString(self, df, n=20, truncate=False, vertical=False):
        """
        Function to store a dataframe as a text
        """
        if isinstance(truncate, bool) and truncate:
            return (df._jdf.showString(n, 20, vertical))
        else:
            return (df._jdf.showString(n, int(truncate), vertical))





