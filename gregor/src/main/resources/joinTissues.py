import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit, regexp_replace, when

# environment
S3_BUCKET = os.getenv('JOB_BUCKET')


def main():
    """
    Arguments: none
    """
    regions_dir = f'{S3_BUCKET}/out/gregor/regions/unsorted/part-*'
    outdir = f'{S3_BUCKET}/out/gregor/regions/joined'

    # this is the schema written out by the regions processor
    regions_schema = StructType(
        [
            StructField('chromosome', StringType(), nullable=False),
            StructField('start', IntegerType(), nullable=False),
            StructField('end', IntegerType(), nullable=False),
            StructField('tissue', StringType(), nullable=False),
            StructField('annotation', StringType(), nullable=False),
            StructField('method', StringType(), nullable=False),
            StructField('predictedTargetGene', StringType(), nullable=False),
            StructField('targetStart', IntegerType(), nullable=False),
            StructField('targetEnd', IntegerType(), nullable=False),
            StructField('transcriptionStartSite', IntegerType(), nullable=False),
            StructField('itemRgb', StringType(), nullable=False),
            StructField('score', DoubleType(), nullable=False),
        ]
    )

    #initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all regions, fix the tissue ID, sort, and write
    regions = spark.read.csv(
        regions_dir,
        sep='\t',
        header=True,
        schema=regions_schema,
    )

    # load the tissue ontology to join
    tissues = spark.read \
        .json(f'{S3_BUCKET}/tissues/ontology') \
        .select(
            col('id').alias('tissueId'),
            col('description').alias('tissue'),
        )

    # convert NA method to null
    method = when(regions.method == 'NA', lit(None)).otherwise(regions.method)

    # fix the tissue and method columns
    regions = regions \
        .withColumn('method', method) \
        .withColumn('tissue', regexp_replace(regions.tissue, '_', ':')) \
        .withColumnRenamed('tissue', 'tissueId')

    # join with the tissue ontology
    df = regions.join(tissues, on='tissueId', how='left_outer')

    # sort by annotation and method
    df.write.mode('overwrite').json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
