import os
from pyspark.sql import SparkSession

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/out/single_cell/graph/*/*/*/*.json.gz'
    df = spark.read.json(srcdir)
    outdir = f'{s3_bioindex}/single_cell/graph/all_edges/'

    df.orderBy(['dataset', 'cell_type', 'model', 'n1_type', 'n1', 'value']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    outdir = f'{s3_bioindex}/single_cell/graph/edges/'

    df.orderBy(['dataset', 'cell_type', 'model', 'n1_type', 'n2_type', 'n1', 'value']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
