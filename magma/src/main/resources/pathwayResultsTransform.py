# imports
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import lit, col, split, input_file_name, regexp_extract

# constants
dir_data = "/home/javaprog/Data/Broad"
dir_data = "s3:/"

# methods
def main():
    """
    Arguments: phenotype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # parse command line
    args = opts.parse_args()
    phenotype = args.phenotype

    # start spark session
    spark = SparkSession.builder.appName('magma').getOrCreate()

    # # EC2 development localhost directories
    file_phenotype_in = f'{dir_data}/dig-analysis-data/out/magma/staging/pathways/{phenotype}/associations.pathways.gsa.out'
    dir_out = f'{dir_data}/dig-analysis-data/out/magma/pathway-associations/{phenotype}'

    # NOTE: This file is whitespace-delimited, which Spark can't properly read.
    #       For this reason, we load it with a dummy delimiter and only get a
    #       single column back. We then split the columns and cast them to the
    #       correct type.

    df = spark.read.csv(file_phenotype_in, sep='^', header=True, comment='#') \
    # .filter(lambda line: len(line)>=4)
    # .withColumn('phenotype', regexp_extract('filename', r'pathways/([^/]+)/associations', 1))
    df.show()

    df = df.select(split(df[0], r'\s+'))
    df = df.select(
        df[0][7].alias('pathwayName').cast(StringType()),
        df[0][2].alias('numGenes').cast(IntegerType()),
        df[0][3].alias('beta').cast(DoubleType()),
        df[0][4].alias('betaStdErr').cast(DoubleType()),
        df[0][5].alias('stdErr').cast(DoubleType()),
        df[0][6].alias('pValue').cast(DoubleType()),
        lit(phenotype).alias('phenotype'),
    )

    df.show()
    print("phenotype {} has rows: {} amd columns: {}".format(phenotype, df.count(), len(df.columns)))


    # write the results
    df.write.mode('overwrite').json(dir_out)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
