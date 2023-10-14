from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():

    # read all
    srcdir = f's3://dig-analysis-data/out/ldsc/regions/credible_sets/*/*/part-*'
    outdir = f's3://dig-bio-index/regions/credible_sets'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all the credible sets for the phenotype
    df = spark.read.json(srcdir)

    # sort by credible set and then locus
    df.orderBy(col('phenotype'), col('annotation'), col('tissue'), col('posteriorProbability').desc()) \
        .write \
        .mode('overwrite') \
        .json(f'{outdir}/')

    # done
    spark.stop()


if __name__ == '__main__':
    main()
