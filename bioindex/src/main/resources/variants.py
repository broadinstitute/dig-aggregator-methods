from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, concat_ws, explode, regexp_replace, row_number, split
from pyspark.sql.types import IntegerType


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = 's3://dig-analysis-data/out/varianteffect/common/part-*'
    outdir = 's3://dig-bio-index/variants/common/'

    df = spark.read.json(srcdir)

    # sort by varId
    df.orderBy(['varId']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()

# entry point
if __name__ == '__main__':
    main()
