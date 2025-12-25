from argparse import ArgumentParser
from os import getenv
from platform import python_version
from pyspark.sql import SparkSession


def main():
    """
    Arguments: phenotype
    """
    print(f'PYTHON VERSION = {python_version()}')
    print(f'USER           = {getenv("USER")}')

    #
    # Job steps can pass arguments to the scripts.
    #

    # build argument
    opts = ArgumentParser()
    opts.add_argument('phenotype')

    # args.phenotype will be set
    args = opts.parse_args()

    # should show args.phenotype
    print(args)

    #
    # All these environment variables will be set for you.
    #

    bucket = getenv('JOB_BUCKET')  # e.g. s3://dig-analysis-data
    method = getenv('JOB_METHOD')  # e.g. Test
    stage = getenv('JOB_STAGE')  # e.g. TestStage
    prefix = getenv('JOB_PREFIX')  # e.g. out/Test/TestStage

    print(f'JOB_BUCKET     = {bucket}')
    print(f'JOB_METHOD     = {method}')
    print(f'JOB_STAGE      = {stage}')
    print(f'JOB_PREFIX     = {prefix}')

    #
    # Run the spark job.
    #

    # PySpark steps need to create a spark session
    spark = SparkSession.builder.appName(method).getOrCreate()

    # TODO: spark job here...

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
