import os

from pyspark.sql import SparkSession

s3_in = os.environ['BIOINDEX_PATH']
s3_out = os.environ['BIOINDEX_PATH']
raw_path = '/mnt/var/raw'


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/diff_exp/{{}}/*/'
    outdir = f'{s3_out}/combined_diff_exp/{{}}/'

    bioindices = {
        'gene': ['geneName', 'pValue'],
        'phenotype-tissue': ['phenotype', 'tissue', 'pValue'],
        'phenotype': ['phenotype', 'pValue'],
        'top-phenotype': ['pValue'],
        'tissue': ['tissue', 'pValue'],
        'top-tissue': ['pValue']
    }

    for bioindex, order_by in bioindices.items():
        df = spark.read.json(srcdir.format(bioindex))
        df.orderBy(order_by) \
            .write \
            .mode('overwrite') \
            .json(outdir.format(bioindex))

    # done
    spark.stop()


if __name__ == '__main__':
    main()

