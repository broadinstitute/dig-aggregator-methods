import numpy as np
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, DoubleType
from pyspark.sql.functions import udf, when

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

# schema used by METAL for datasets
GENES_SCHEMA = StructType(
    [
        StructField('gene', StringType()),
        StructField('species', StringType()),
        StructField('datasetType', StringType()),
        StructField('datasetId', StringType()),
        StructField('datasetRef', StringType()),
        StructField('log_fold_change', DoubleType()),
        StructField('p_value', DoubleType()),
        StructField('p_value_adj', DoubleType())
    ]
)


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/single_cell_genes/all_sig_genes.all_datasets.tsv.gz'
    outdir = f'{s3_bioindex}/single_cell/genes'

    df = spark.read.csv(srcdir, header=True, sep='\t', schema=GENES_SCHEMA)

    gene_to_upper = udf(lambda s: s.upper())

    # extract the dataset and ancestry from the filename
    df = df.withColumn('gene', gene_to_upper(df.gene))
    df = df.withColumn('p_value', when(df.p_value == 0.0, np.nextafter(0, 1)).otherwise(df.p_value))
    df = df.withColumn('p_value_adj', when(df.p_value_adj == 0.0, np.nextafter(0, 1)).otherwise(df.p_value_adj))

    # write associations sorted by variant and then pValue
    df.orderBy(['gene', 'p_value_adj']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
