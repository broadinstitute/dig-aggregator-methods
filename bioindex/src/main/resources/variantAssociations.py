import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import input_file_name, udf

# schema used by METAL for datasets
VARIANTS_SCHEMA = StructType(
    [
        StructField('varId', StringType()),
        StructField('chromosome', StringType()),
        StructField('position', IntegerType()),
        StructField('reference', StringType()),
        StructField('alt', StringType()),
        StructField('phenotype', StringType()),
        StructField('pValue', DoubleType()),
        StructField('beta', DoubleType()),
        StructField('stdErr', DoubleType()),
        StructField('n', DoubleType()),
    ]
)


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load and output directory
    srcdir = f's3://dig-analysis-data/out/metaanalysis/variants/*/*/*/*/part-*'
    outdir = f's3://dig-bio-index/associations/variant'

    # load input associations to the bottom line across phenotypes
    df = spark.read.csv(srcdir, header=True, sep='\t', schema=VARIANTS_SCHEMA)

    # input filename -> dataset name
    dataset_of_input = udf(lambda s: s and re.search(r'/dataset=([^/]+)/', s).group(1))

    # extract the dataset and ancestry from the filename
    df = df.withColumn('dataset', dataset_of_input(input_file_name()))

    # keep only the data needed across datasets for a phenotype
    df = df.select(
        df.varId,
        df.chromosome,
        df.position,
        df.dataset,
        df.phenotype,
        df.pValue,
        df.beta,
        df.n,
    )

    # write associations sorted by variant and then pValue
    df.orderBy(['chromosome', 'position', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
