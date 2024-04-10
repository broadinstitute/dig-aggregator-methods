import os
from pyspark.sql import SparkSession

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']


def main():
    # PySpark steps need to create a spark session
    spark = SparkSession.builder.appName('Bioindex').getOrCreate()

    # src and output
    srcdir = f'{s3_in}/effector_genes/*/part-*'
    outdir = f'{s3_bioindex}/effector_genes'

    # load all the effector gene datasets
    df = spark.read.json(srcdir)

    # sort them by phenotype, dataset, and then gene name
    df.orderBy(['phenotype', 'dataset', 'prediction']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/phenotype')

    # sort by gene, dataset, and then phenotype
    df.orderBy(['gene', 'dataset', 'phenotype']) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/gene')

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
