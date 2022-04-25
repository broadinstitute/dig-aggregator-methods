#!/usr/bin/python3

import argparse
import os.path
import platform
import re
import subprocess

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, isnan, lit  # pylint: disable=E0611

# where in S3 meta-analysis data is
s3_bucket = 's3://psmadbec-test/bottom-line-without-largest'
s3_path = '%s/out/metaanalysis' % s3_bucket
s3_staging = '%s/staging' % s3_path

# where local analysis happens
localdir = '/mnt/var/metal'

# this is the schema written out by the variant partition process
variants_schema = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('reference', StringType(), nullable=False),
        StructField('alt', StringType(), nullable=False),
        StructField('phenotype', StringType(), nullable=False),
        StructField('pValue', DoubleType(), nullable=False),
        StructField('beta', DoubleType(), nullable=False),
        StructField('stdErr', DoubleType(), nullable=False),
        StructField('n', DoubleType(), nullable=False),
    ]
)


def metaanalysis_schema(samplesize=True, overlap=False):
    """
    Create the CSV schema used for the METAANALYSIS output file.
    """
    schema = [
        StructField('MarkerName', StringType(), nullable=False),
        StructField('Allele1', StringType(), nullable=False),
        StructField('Allele2', StringType(), nullable=False),
    ]

    # add samplesize or stderr
    if samplesize:
        schema += [
            StructField('Weight', DoubleType(), nullable=False),
            StructField('Zscore', DoubleType(), nullable=False),
        ]
    else:
        schema += [
            StructField('Effect', DoubleType(), nullable=False),
            StructField('StdErr', DoubleType(), nullable=False),
        ]

    # add N if overlap was ON
    if samplesize and overlap:
        schema += [
            StructField('N', DoubleType(), nullable=False),
        ]

    # add p-value and direction
    schema += [
        StructField('Pvalue', DoubleType(), nullable=False),
        StructField('Direction', StringType(), nullable=False),
        StructField('TotalSampleSize', DoubleType(), nullable=False),
    ]

    return StructType(schema)


def read_samplesize_analysis(spark, path, overlap):
    """
    Read the output of METAL when run with OVERLAP ON and SCHEME SAMPLESIZE.
    """

    def transform(row):
        chrom, pos, ref, alt = row.MarkerName.split(':')

        # sometimes METAL will flip the alleles
        flip = alt == row.Allele1.upper()

        return Row(
            varId=row.MarkerName,
            chromosome=chrom,
            position=int(pos),
            reference=ref,
            alt=alt,
            pValue=row.Pvalue,
            zScore=row.Zscore if not flip else -row.Zscore,
            n=row.TotalSampleSize,
        )

    # load into spark and transform
    return spark.read \
        .csv(
            path,
            sep='\t',
            header=True,
            schema=metaanalysis_schema(samplesize=True, overlap=overlap),
        ) \
        .filter(col('MarkerName').isNotNull()) \
        .rdd \
        .map(transform) \
        .toDF() \
        .filter(isnan(col('pValue')) == False) \
        .filter(isnan(col('zScore')) == False)


def read_stderr_analysis(spark, path):
    """
    Read the output of METAL when run with OVERLAP OFF and SCHEMA STDERR.
    """

    def transform(row):
        _, _, _, alt = row.MarkerName.upper().split(':')

        # sometimes METAL will flip the alleles
        flip = alt == row.Allele1.upper()

        # # get the effect allele frequency and minor allele frequency
        # eaf = row.Freq1 if not flip else 1.0 - row.Freq1
        # maf = eaf if eaf < 0.5 else 1.0 - eaf

        return Row(
            varId=row.MarkerName.upper(),
            # eaf=eaf,
            # maf=maf,
            beta=row.Effect if not flip else -row.Effect,
            stdErr=row.StdErr,
        )

    # load into spark and transform
    return spark.read \
        .csv(
            path,
            sep='\t',
            header=True,
            schema=metaanalysis_schema(samplesize=False, overlap=False),
        ) \
        .filter(col('MarkerName').isNotNull()) \
        .rdd \
        .map(transform) \
        .toDF() \
        .filter(isnan(col('beta')) == False) \
        .filter(isnan(col('stdErr')) == False)


def load_analysis(spark, path, overlap=False):
    """
    Load the SAMPLESIZE and STDERR analysis and join them together.
    """
    samplesize_outfile = '%s/scheme=SAMPLESIZE/METAANALYSIS1.tbl' % path
    stderr_outfile = '%s/scheme=STDERR/METAANALYSIS1.tbl' % path

    # load both files into data frames
    samplesize_analysis = read_samplesize_analysis(spark, samplesize_outfile, overlap)
    stderr_analysis = read_stderr_analysis(spark, stderr_outfile)

    # join the two analyses together by id
    return samplesize_analysis.join(stderr_analysis, 'varId')


def hadoop_ls(path):
    """
    Run `hadoop fs -ls -C` to find all the files that match a particular path.
    """
    try:
        return subprocess.check_output(['hadoop', 'fs', '-ls', '-C', path]) \
            .decode('UTF-8') \
            .strip() \
            .split('\n')
    except subprocess.CalledProcessError:
        return []


def hadoop_test(path):
    """
    Run `hadoop fs -test -s` to see if any files exist matching the pathspec.
    """
    return len(hadoop_ls(path)) > 0


def load_ancestry_specific_analysis(phenotype):
    """
    Load the METAL results for each ancestry into a single DataFrame.
    """
    srcdir = '%s/ancestry-specific/%s' % (s3_staging, phenotype)
    outdir = '%s/ancestry-specific/%s' % (s3_path, phenotype)

    # the final dataframe for each ancestry
    ancestries = set()

    # discover all the ancestries
    for tbl in hadoop_ls('%s/*/*/METAANALYSIS1.tbl' % srcdir):
        ancestry = re.search(r'/ancestry=([^/]+)/', tbl).group(1)
        ancestries.add(ancestry)

    # for each ancestry, load the analysis
    for ancestry in ancestries:
        print('Loading ancestry %s...' % ancestry)

        path = '%s/ancestry=%s' % (srcdir, ancestry)

        # NOTE: The columns from the analysis and rare variants need to be
        #       in the same order before unioning the sets together. To
        #       guarantee this, we'll select from each using the schema written
        #       by the partition variants script.

        columns = [col(field.name) for field in variants_schema]

        # read the analysis produced by METAL
        df = load_analysis(spark, path, overlap=True)

        # add ancestry for partitioning and calculated eaf/maf averages
        df = df \
            .withColumn('phenotype', lit(phenotype)) \
            .select(*columns)

        # rare variants across all datasets for this phenotype and ancestry
        rare_path = '%s/variants/%s/*/ancestry=%s/rare=true' % (s3_path, phenotype, ancestry)

        # are there rare variants to merge with the analysis?
        if hadoop_test(rare_path):
            print('Merging rare variants...')

            # load the rare variants across all datasets
            rare_variants = spark.read \
                .csv(rare_path, sep='\t', header=True, schema=variants_schema) \
                .select(*columns)

            # update the analysis and keep variants with the largest N
            df = df.union(rare_variants) \
                .rdd \
                .keyBy(lambda v: v.varId) \
                .reduceByKey(lambda a, b: b if (b.n or 0) > (a.n or 0) else a) \
                .map(lambda v: v[1]) \
                .toDF()

        # write the analysis out, manually partitioned
        df.write \
            .mode('overwrite') \
            .json('%s/ancestry=%s' % (outdir, ancestry))


def load_trans_ethnic_analysis(phenotype):
    """
    The output from each ancestry-specific analysis is pulled together and
    processed with OVERLAP OFF. Once done, the results are uploaded back to
    HDFS (S3) where they can be kept and uploaded to a database.
    """
    srcdir = '%s/trans-ethnic/%s' % (s3_staging, phenotype)
    outdir = '%s/trans-ethnic/%s' % (s3_path, phenotype)

    print("Loading from {} to {}".format(srcdir, outdir))

    if not hadoop_test(srcdir):
        return

    # load the analyses - note that zScore is present for trans-ethnic!
    variants = load_analysis(spark, srcdir, overlap=False) \
        .withColumn('phenotype', lit(phenotype)) \
        .filter(col('pValue') > 0.0) \
        .select(
            'varId',
            'chromosome',
            'position',
            'reference',
            'alt',
            'phenotype',
            'pValue',
            'beta',
            'zScore',
            'stdErr',
            'n',
        )

    print("Number of variants: {}".format(variants.count()))

    # find all the unique ancestries that went into this analysis
    datasets = spark.read.json(f'{s3_bucket}/variants/*/*/{phenotype}/metadata')
    ancestries = datasets.select(datasets.ancestry).distinct().collect()

    # NOTE: If non-mixed ancestries were used in the analysis, then Mixed
    #       ancestries were NOT used. We need to now load the mixed ancestry variants
    #       from those datasets not present in the bottom-line, add them to the
    #       variants dataframe, group by varId, and choose the result with max(n)

    if (len(ancestries) > 1) or ('Mixed' not in ancestries):
        print(f'Adding unique Mixed variants to bottom-line results for {phenotype}')

        # load all the mixed ancestry-variants across the datasets
        mixed = spark.read.json(f'{s3_bucket}/variants/*/*/{phenotype}/part-*')
        mixed = mixed.filter(mixed.ancestry == 'Mixed') \
            .select(
                'varId',
                'chromosome',
                'position',
                'reference',
                'alt',
                'phenotype',
                'pValue',
                'beta',
                'zScore',
                'stdErr',
                'n',
            )
        print("Number of mixed variants: {}".format(mixed.count()))

        # add mixed to variants and keep variants with the largest n
        variants = variants.union(mixed) \
            .rdd \
            .keyBy(lambda v: v.varId) \
            .reduceByKey(lambda a, b: b if (b.n or 0) > (a.n or 0) else a) \
            .map(lambda v: v[1]) \
            .toDF()

        print("Number of combined variants: {}".format(variants.count()))

    # write the variants
    variants.write.mode('overwrite').json(outdir)


# entry point
if __name__ == '__main__':
    """
    Arguments: [--ancestry-specific | --trans-ethnic] <phenotype>

    Either --ancestry-specific or --trans-ethnic is required to be passed on
    the command line, but they are mutually exclusive.
    """
    print('python version=%s' % platform.python_version())
    print('user=%s' % os.getenv('USER'))

    opts = argparse.ArgumentParser()
    opts.add_argument('--ancestry-specific', action='store_true', default=False)
    opts.add_argument('--trans-ethnic', action='store_true', default=False)
    opts.add_argument('phenotype')

    # parse command line arguments
    args = opts.parse_args()

    # --ancestry-specific or --trans-ethnic must be provided, but not both!
    assert args.ancestry_specific != args.trans_ethnic

    # create the spark context
    spark = SparkSession.builder.appName('bottom-line').getOrCreate()

    # either run the trans-ethnic analysis or ancestry-specific analysis
    if args.ancestry_specific:
        load_ancestry_specific_analysis(args.phenotype)
    else:
        load_trans_ethnic_analysis(args.phenotype)

    # done
    spark.stop()
