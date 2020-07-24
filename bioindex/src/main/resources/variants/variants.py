import os

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType
from pyspark.sql.functions import col, struct

# what bucket will be output to?
OUT_BUCKET = f'dig-bio-{"test" if os.getenv("JOB_DRYRUN") else "index"}'

# variant list schema
VARIANTS_SCHEMA = StructType(
    [
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('end', IntegerType(), nullable=False),
        StructField('allele', StringType(), nullable=False),
        StructField('strand', StringType(), nullable=False),
        StructField('varId', StringType(), nullable=False),
    ]
)

# this is the schema written out by the frequency analysis processor
FREQ_SCHEMA = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('reference', StringType(), nullable=False),
        StructField('alt', StringType(), nullable=False),
        StructField('eaf', DoubleType(), nullable=False),
        StructField('maf', DoubleType(), nullable=False),
        StructField('ancestry', StringType(), nullable=False),
    ]
)

# this is the schema written out by the meta-analysis processor
ASSOC_SCHEMA = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('reference', StringType(), nullable=False),
        StructField('alt', StringType(), nullable=False),
        StructField('phenotype', StringType(), nullable=False),
        StructField('pValue', DoubleType(), nullable=False),
        StructField('beta', DoubleType(), nullable=False),
        StructField('zScore', DoubleType(), nullable=False),
        StructField('stdErr', DoubleType(), nullable=False),
        StructField('n', DoubleType(), nullable=False),
        StructField('top', BooleanType(), nullable=False),
    ]
)


def load_freq(spark, ancestry):
    """
    Loads all the frequency data for a given ancestry.
    """
    src = 's3://dig-analysis-data/out/frequencyanalysis/%s/part-*' % ancestry

    return spark.read \
        .csv(src, sep='\t', header=True, schema=FREQ_SCHEMA) \
        .select(col('varId'), struct('eaf', 'maf').alias(ancestry))


def main():
    """
    Arguments: none
    """
    variants_srcdir = 's3://dig-analysis-data/out/varianteffect/variants/part-*'
    assoc_srcdir = 's3://dig-analysis-data/out/metaanalysis/trans-ethnic/*/part-*'
    common_srcdir = 's3://dig-analysis-data/out/varianteffect/common/part-*'

    # output location
    outdir = f's3://{OUT_BUCKET}/variants'

    # initialize spark session
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all the unique variants
    variants = spark.read.csv(variants_srcdir, sep='\t', header=False, schema=VARIANTS_SCHEMA) \
        .select('varId', 'chromosome', 'position')

    # frequency outputs by ancestry
    ancestries = ['AA', 'EA', 'EU', 'HS', 'SA']
    freq = None

    # load frequencies by variant ID
    for ancestry in ancestries:
        df = load_freq(spark, ancestry)

        # final, joined frequencies
        freq = df if freq is None else freq.join(df, 'varId', how='outer')

    # pull all the frequencies together into a single map
    freq = freq.select(freq.varId, struct(*ancestries).alias('frequency'))

    # common effect data from VEP
    common = spark.read.csv(common_srcdir, sep='\t', header=True)

    # load transcription factors and group them by varId
    tfs = spark.read.csv(tf_srcdir, sep='\t', header=True, schema=TF_SCHEMA) \
        .rdd \
        .keyBy(lambda r: r.varId) \
        .combineByKey(lambda r: [r], lambda c, r: c + [r], lambda c, rs: c + rs) \
        .toDF() \
        .select(
            col('_1').alias('varId'),
            col('_2').alias('transcriptionFactors'),
        )

    # load the consequences
    cqs = spark.read.json(vep_srcdir) \
        .select(
            col('id').alias('varId'),
            col('transcript_consequences').alias('transcriptConsequences'),
        )

    # load the bottom-line results, join them together by varId
    assoc = spark.read.csv(assoc_srcdir, sep='\t', header=True, schema=ASSOC_SCHEMA) \
        .select(
            col('varId'),
            col('phenotype'),
            col('pValue'),
            col('beta'),
            col('zScore'),
            col('stdErr'),
            col('n'),
        ) \
        .rdd \
        .keyBy(lambda r: r.varId) \
        .aggregateByKey([], lambda a, b: a + [b], lambda a, b: a + b) \
        .toDF() \
        .select(
            col('_1').alias('varId'),
            col('_2').alias('associations'),
        )

    # join everything together
    df = variants \
        .join(common, 'varId', how='left_outer') \
        .join(freq, 'varId', how='left_outer') \
        .join(assoc, 'varId', how='left_outer') \
        .join(tfs, 'varId', how='left_outer') \
        .join(cqs, 'varId', how='left_outer')

    # write out variant data by ID
    df.orderBy(['chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
