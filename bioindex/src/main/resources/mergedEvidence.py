import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, input_file_name, udf

s3_in = os.environ['INPUT_PATH']
s3_out = os.environ['OUTPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

outdir = f'{s3_bioindex}/merged_evidence/{{}}'

schema = StructType([
    StructField('gene', StringType(), nullable=False),
    StructField('HGNC', StringType(), nullable=False),
    StructField('gene_type', StringType(), nullable=False),
    StructField('varID', StringType(), nullable=False),
    StructField('posteriorProbability', DoubleType(), nullable=False),
    StructField('pValue', DoubleType(), nullable=False),
    StructField('credibleSetId', StringType(), nullable=False),
    StructField('csNumVar', IntegerType(), nullable=False),
    StructField('csMaxPIP', DoubleType(), nullable=False),
    StructField('csMaxPIPVar', StringType(), nullable=False),
    StructField('trueMaxVar', BooleanType(), nullable=False),
    StructField('entropy', DoubleType(), nullable=False),
    StructField('clusters', StringType(), nullable=False),
    StructField('cluster_loadings', StringType(), nullable=False),
    StructField('tissue', StringType(), nullable=False),
    StructField('annotation', StringType(), nullable=False),
    StructField('targetSource', StringType(), nullable=False),
    StructField('variantInTarget', BooleanType(), nullable=False),
    StructField('prioritized', IntegerType(), nullable=False),
    StructField('exome', IntegerType(), nullable=False),
    StructField('omin', IntegerType(), nullable=False),
    StructField('prioritized_neighbor', IntegerType(), nullable=False),
    StructField('exome_neighbor', IntegerType(), nullable=False),
    StructField('omin_neighbor', IntegerType(), nullable=False)
])

src_re = r'merged_evidence/([^\.]+)\.([^\.]+)\..*'
phenotype = udf(lambda s: re.search(src_re, s).group(1))
ancestry = udf(lambda s: re.search(src_re, s).group(2))

def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/merged_evidence/*'
    df = spark.read.csv(srcdir, sep='\t', header=True, schema=schema) \
        .withColumn('file_name', input_file_name()) \
        .withColumn('phenotype', phenotype('file_name')) \
        .withColumn('ancestry', ancestry('file_name'))
    df = df.drop('file_name')

    df.orderBy(['phenotype', 'ancestry', 'tissue', col('posteriorProbability').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('tissue'))

    df.orderBy(['phenotype', 'ancestry', 'tissue', 'gene', col('posteriorProbability').desc()]) \
        .write \
        .mode('overwrite') \
        .json(outdir.format('tissue-gene'))

    spark.stop()

if __name__ == '__main__':
    main()
