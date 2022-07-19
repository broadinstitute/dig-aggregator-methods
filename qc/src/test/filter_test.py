from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
import unittest

from qc.src.main.resources.variantQC import filters_to_run


class TestFilters(unittest.TestCase):
    def setUp(self):
        self.conf = SparkConf().set("spark.driver.bindAddress", "127.0.0.1")
        self.spark = SparkSession.builder.master("local").appName("local").config(conf=self.conf).getOrCreate()

    def get_dataframe(self, column_name, column_value):
        return self.spark.createDataFrame(
            [(column_value,)],
            StructType([StructField(column_name, StringType(), True)])
        )

    def check_count(self, value, df, target):
        self.assertEqual((value, df.count()), (value, target))

    def check_column(self, column_name, good_values, bad_values):
        checks = [a for a in filters_to_run if a.column_name == column_name]
        self.assertEqual(len(checks), 1)

        for bv in bad_values:
            df_bad, df_good = checks[0].split(
                self.get_dataframe(column_name, bv)
            )
            self.check_count(bv, df_good, 0)
            self.check_count(bv, df_bad, 1)

        for gv in good_values:
            df_bad, df_good = checks[0].split(
                self.get_dataframe(column_name, gv)
            )
            self.check_count(gv, df_good, 1)
            self.check_count(gv, df_bad, 0)

    def test_chromosome(self):
        bad_chromosomes = ["25", "100", "010", "Z", "1 ", "0", "1.1", "1E1", "", None]
        good_chromosomes = [str(i) for i in range(1, 25)] + ["X", "Y"]
        self.check_column('chromosome', good_chromosomes, bad_chromosomes)

    def test_position(self):
        bad_positions = ["-1", "1.2", "1E3", "1e4", "s", " 1", "010", "", None]
        good_positions = ["0", "1", "10", "123456789"]
        self.check_column('position', good_positions, bad_positions)

    def test_alt_ref(self):
        bad_bases = ["U", "u", "0", "1", "A ", "", None]
        good_bases = ["A", "T", "C", "G", "a", "t", "g", "c", "ATCGatcg"]
        self.check_column('alt', good_bases, bad_bases)
        self.check_column('reference', good_bases, bad_bases)

    def test_pvalue(self):
        bad_pvalue = ["-0.1", "1.1", "1E-3E-4", "0.3.4", "0.1 ", "", None]
        good_pvalue = ["0", "1", "0.1", "1E-1", "1e-1"]
        self.check_column('pValue', good_pvalue, bad_pvalue)

    def test_odds_eaf(self):
        bad_odds = ["-0.1", "1.1", "1E-3E-4", "0.3.4", "0.1 ", ""]
        good_odds = ["0", "1", "0.1", "1E-1", "1e-1", None]
        self.check_column('oddsRatio', good_odds, bad_odds)
        self.check_column('eaf', good_odds, bad_odds)

    def test_beta(self):
        bad_beta = ["1E-3E-4", "0.3.4", "0.1 ", ""]
        good_beta = ["-1", "0", "1", "-1.1", "-0.1", "1.1", "0.1", "1E-1", "1e-1", "-1E-1", "-1e-1", None]
        self.check_column('beta', good_beta, bad_beta)

    def test_stderr(self):
        bad_stderr = ["-0.1", "1E-3E-4", "0.3.4", "0.1 ", ""]
        good_stderr = ["0", "1", "0.1", "1.1", "1E-1", "1e-1", None]
        self.check_column('stdErr', good_stderr, bad_stderr)

    def test_n(self):
        bad_stderr = ["-0.1", "1E-3E-4", "0.3.4", "0.1 ", ""]
        good_stderr = ["0", "1", "0.1", "1.1", "1E-1", "1e-1", None]
        self.check_column('n', good_stderr, bad_stderr)
