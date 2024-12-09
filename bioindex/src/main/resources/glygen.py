import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, concat_ws, explode, regexp_replace, row_number, split
from pyspark.sql.types import IntegerType

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/genes_data/glycosylated_genes.json'
    outdir = f'{s3_bioindex}/glygen/'

    df = spark.read.json(srcdir)

    # sort by chromosome, position
    df.orderBy(['gene_symbol']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    spark.stop()

# entry point
if __name__ == '__main__':
    main()
