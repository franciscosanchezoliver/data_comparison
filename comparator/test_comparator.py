from spark_utils import PySparkTestCase
from pyspark.sql import functions as f
from comparator import Comparator


class ComparatorTest(PySparkTestCase):

    def test_get_columns_to_compare(self):
        comp = Comparator(df_a=self.df_a,
                          df_b=self.df_b,
                          common_key="identifier",
                          tag_for_a="old",
                          tag_for_b="edp")

        columns_to_compare = comp.get_common_columns()

        self.assertEqual(columns_to_compare,
                         ['address', 'n_rooms', 'operation', 'periodicity', 'proba_selling'])

    def test_rename_columns(self):
        comp = Comparator(df_a=self.df_a,
                          df_b=self.df_b,
                          common_key="identifier",
                          tag_for_a="old",
                          tag_for_b="edp")

        comp.rename_columns()
        self.assertTrue(all(['_old' in col for col in comp.df_a.columns]))
        self.assertTrue(all(['_edp' in col for col in comp.df_b.columns]))

    def test_get_commons_rows(self):
        comp = Comparator(df_a=self.df_a,
                          df_b=self.df_b,
                          common_key="identifier",
                          tag_for_a="old",
                          tag_for_b="edp")
        comp.rename_columns()
        comp.get_commons_rows()
        common_ids = comp.commons.select("identifier_old").collect()
        common_ids = [row[0] for row in common_ids]
        self.assertEqual(common_ids, [5, 6, 7, 8])

    def test_check_equality_between_columns(self):
        comp = Comparator(df_a=self.df_a,
                          df_b=self.df_b,
                          common_key="identifier",
                          tag_for_a="old",
                          tag_for_b="edp")
        comp.get_common_columns()
        comp.rename_columns()
        comp.get_commons_rows()
        comp.check_equality_between_columns()

        true_count = comp.commons.filter(f.col("n_rooms_equals")).count()
        total_count = comp.commons.count()

        self.assertEqual(true_count/total_count, 0.5)

    def test_calculate_stats_of_comparison(self):
        comp = Comparator(df_a=self.df_a,
                          df_b=self.df_b,
                          common_key="identifier",
                          tag_for_a="old",
                          tag_for_b="edp")
        comp.get_common_columns()
        comp.rename_columns()
        comp.get_commons_rows()
        comp.check_equality_between_columns()
        comp.calculate_stats_of_comparison()

        self.assertEqual(comp.stats['periodicity_equals']['Null_percentage'], 0.25)
        self.assertEqual(comp.stats['proba_selling_equals'][True], 2)

    def test_create_spark_df_with_stats(self):
        comp = Comparator(df_a=self.df_a,
                          df_b=self.df_b,
                          common_key="identifier",
                          tag_for_a="old",
                          tag_for_b="edp")

        comp.get_common_columns()
        comp.rename_columns()
        comp.get_commons_rows()
        comp.check_equality_between_columns()
        comp.calculate_stats_of_comparison()
        comp.create_spark_df_with_stats()
        self.assertEqual(comp.df_comparison.count(), 5)

    def test_generate_differences_report_as_string(self):
        comp = Comparator(df_a=self.df_a,
                          df_b=self.df_b,
                          common_key="identifier",
                          tag_for_a="old",
                          tag_for_b="edp")

        comp.get_common_columns()
        comp.rename_columns()
        comp.get_commons_rows()
        comp.check_equality_between_columns()
        comp.calculate_stats_of_comparison()
        comp.create_spark_df_with_stats()
        comp.generate_differences_report_as_string()
        self.assertTrue(len(comp.diffences_report_as_txt) > 1000)

    def test_remove_columns_with_different_data_types(self):
        comp = Comparator(df_a=self.df_a,
                          df_b=self.df_b,
                          common_key="identifier",
                          tag_for_a="old",
                          tag_for_b="edp")

        comp.get_common_columns()
        comp.remove_columns_with_different_data_types()
        self.assertFalse('n_rooms' in comp.columns_to_compare)

    def test_complete_compare(self):
        comp = Comparator(df_a=self.df_a,
                              df_b=self.df_b,
                              common_key="identifier",
                              tag_for_a="old",
                              tag_for_b="edp")

        comp.compare()
        print("hello")
