#!/usr/bin/python3

import argparse
import platform
import subprocess

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, input_file_name, regexp_extract  # pylint: disable=E0611
from pyspark.sql.types import StructType, StructField, StringType

S3DIR = 's3://dig-analysis-data'

# GREGOR doesn't work on XY, M, or MT chromosomes.
CHROMOSOMES = list(map(lambda c: str(c + 1), range(22))) + ['X', 'Y']
ANCESTRIES = ['AA', 'AF', 'EA', 'EU', 'HS', 'SA', 'Mixed']


def test_glob(glob):
    """
    Searches for part files using a path glob.
    """
    cmd = ['hadoop', 'fs', '-test', '-e', glob]
    print('Running: ' + str(cmd))

    # if it returns non-zero, then the files didn't exist
    status = subprocess.call(cmd)
    print('Return code: ' + str(status))

    return status == 0


def clump(snps, chrom, ancestry):
    """
    Returns the most significant variants for an ancestry on the given
    chromosome for 1 mb regions.
    """
    snps = [[snp, True] for snp in snps if snp.chromosome == chrom and snp.ancestry == ancestry]

    # final output list and current index
    output_snps = []
    i = 0

    # loop until all the snps are written to output or dropped
    while i < len(snps):
        print(f'Writing {snps[i][0]}')

        # add to the output
        pos = snps[i][0].position
        output_snps.append(Row(SNP=f'{chrom}:{pos}', ancestry=ancestry))

        # clear available flag for nearby snps
        next_i = None
        for n in range(i, len(snps)):
            if snps[n][1]:
                if abs(snps[n][0].position - snps[i][0].position) <= 500000:
                    snps[n][1] = False
                elif not next_i:
                    next_i = n

        # advance to the next variant
        i = next_i or len(snps)

    return output_snps


def main():
    """
    Arguments: <phenotype>
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # get command line arguments
    args = opts.parse_args()

    # get the source and output directories
    srcdir = '%s/out/metaanalysis/ancestry-specific/%s/*/part-*' % (S3DIR, args.phenotype)
    outdir = '%s/out/gregor/snp/%s' % (S3DIR, args.phenotype)

    spark = SparkSession.builder.appName('gregor').getOrCreate()

    # The algorithm here is to find all the best SNPs on each chromosome, but only
    # SNPs within a given range of each other (e.g. 500 kb). The following
    # algorithm is used:
    #
    #   1. Create an empty DataFrame of SNPs that will serve as the output
    #   2. For each chromosome:
    #      a. find all SNPs < p-value threshold (source DF)
    #      b. find the best SNP (lowest p-value) a put it into the output DF
    #      c. remove all SNPs within the given range (bp) from the source DF
    #      d. if the source DF is not empty, goto 2b.
    #   3. The output DF now contains all the SNPs to use.

    output_schema = StructType([
        StructField('SNP', StringType()),
        StructField('ancestry', StringType()),
    ])

    # create the output SNP data frame
    output_SNPs = []

    # NOTE: It's possible that srcdir doesn't actually exist or contain anything!
    #       This is because meta-analysis filters data. For example, perhaps a
    #       dataset doesn't contain any BETA or P-VALUE data, in which case it
    #       will be successfully run through the meta-analysis processor, but it
    #       won't write anything out or get through ancestry-specific analysis.
    #
    #       When that happens, it's OK to just have an empty output SNP list.

    if test_glob(srcdir):
        df = spark.read.json(srcdir) \
            .withColumn('filename', input_file_name()) \
            .withColumn('ancestry', regexp_extract('filename', r'/ancestry=([^/]+)/', 1))

        # keep only the columns needed, and the globally-significant variants
        df = df.select(df.chromosome, df.position, df.pValue, df.ancestry)
        df = df.filter(df.pValue <= 5e-8)
        df = df.sort(df.pValue)

        # get all the sorted variants on the master node
        snps = df.collect()

        # append the clumped variants for each chromosome/ancestry pair
        for chrom in CHROMOSOMES:
            for ancestry in ANCESTRIES:
                output_snps += clump(snps, chrom, ancestry)

    # output the variants as CSV part files for GREGOR
    spark.createDataFrame(output_SNPs, output_schema) \
        .write \
        .mode('overwrite') \
        .partitionBy('ancestry') \
        .csv(outdir, sep='\t')

    # done
    spark.stop()


# entry point
if __name__ == '__main__':
    main()
