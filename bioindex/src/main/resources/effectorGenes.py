from os import getenv
from platform import python_version
from pyspark.sql import SparkSession


def main():
    """
    Arguments: none
    """
    print(f'PYTHON VERSION = {python_version()}')
    print(f'USER           = {getenv("USER")}')

    # load environment variables
    bucket = getenv('JOB_BUCKET')  # e.g. s3://dig-analysis-data
    method = getenv('JOB_METHOD')  # e.g. Test

    # PySpark steps need to create a spark session
    spark = SparkSession.builder.appName(method).getOrCreate()

    # src and output
    srcdir = f'{bucket}/effector_genes/*/part-*'
    outdir = f'dig-bio-{"test" if getenv("JOB_DRYRUN") else "index"}/effector_genes'

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
