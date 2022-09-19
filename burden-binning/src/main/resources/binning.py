import functools

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode, lit, when


CQS_SRCDIR = 's3://dig-analysis-data/out/varianteffect/cqs'
VARIANTS_SRCDIR = 's3://dig-analysis-data/variants'
TECH_SOURCES = ['ExSeq', 'WGS']


def load_vep_consequences(spark):
    """
    Loads the VEP data, explodes consequences, and picks the best one.
    """
    df = spark.read.json(f'{CQS_SRCDIR}/part-*')
    df = df.filter(df.pick == 1)

    # only keep specific fields
    return df.select(
        df.varId,
        df.geneId,
        df.geneSymbol,
        df.transcriptId,
        df.lof,
        df.impact,
        df.polyphen2HdivPred,
        df.polyphen2HvarPred,
        df.proveanPred,
        df.siftPred,
        df.mutationtasterPred,
        df.lrtPred,
        df.metalrPred,
        df.fathmmPred,
        df.fathmmMklCodingPred,
        df.eigenPcRawCodingRankscore,
        df.dannRankscore,
        df.vest4Rankscore,
        df.caddRawRankscore,
        df.metasvmPred,
        df.mCapScore,
        df.gnomadGenomesPopmaxAf,
    )


def load_unique_variants(spark):
    """
    Only load variants from specific tech datasets.
    """
    df = None

    for tech in TECH_SOURCES:
        srcdir = f'{VARIANTS_SRCDIR}/{tech}/*/*/part-*'

        # get just the list of unique variants
        variants = spark.read.json(srcdir) \
            .select('varId')

        # union all the variants together
        df = df.unionAll(variants) if df is not None else variants

    # only unique variants
    return df.dropDuplicates(['varId'])


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('burdenbinning').getOrCreate()

    # where to output the results to
    outdir = 's3://dig-analysis-data/out/burdenbinning'

    # load input data
    variants = load_unique_variants(spark)
    cqs = load_vep_consequences(spark)

    # keep only the consequence output for the variants we care about
    df = variants.join(cqs, on='varId', how='inner')

    # calculate MAF from AF
    maf = when(df.gnomadGenomesPopmaxAf < 0.5, df.gnomadGenomesPopmaxAf) \
        .otherwise(1.0 - df.gnomadGenomesPopmaxAf)

    # add MAF column
    df = df.withColumn('maf', maf)

    # 0/5 predictor masks
    pred_0of5 = (df.impact == 'MODERATE') & (df.maf < 0.01)

    # 1/5 predictor masks
    pred_1of5 = pred_0of5 & (
        (df.polyphen2HdivPred.contains('D')) | \
        (df.polyphen2HvarPred.contains('D')) | \
        (df.siftPred.contains('D')) | \
        (df.lrtPred.contains('D')) | \
        (df.mutationtasterPred.contains('A') | df.mutationtasterPred.contains('D')) \
    )

    # 5/5 predictor masks
    pred_5of5 = \
        (df.polyphen2HdivPred.contains('D')) & \
        (df.polyphen2HvarPred.contains('D')) & \
        (df.siftPred.contains('D')) & \
        (df.lrtPred.contains('D')) & \
        (df.mutationtasterPred.contains('A') | df.mutationtasterPred.contains('D'))

    # 5/5 + 6 more masks
    pred_11of11 = pred_5of5 & \
        (df.metalrPred.contains('D')) & \
        (df.metasvmPred.contains('D')) &  \
        (df.proveanPred.contains('D')) & \
        (df.fathmmMklCodingPred.contains('D')) & \
        (df.fathmmPred.contains('D')) & \
        (df.mCapScore >= 0.025)

    # 11/11 + 4 more masks
    pred_16of16 = pred_11of11 & \
        (df.eigenPcRawCodingRankscore >= 0.9) & \
        (df.dannRankscore >= 0.9) & \
        (df.caddRawRankscore >= 0.9) & \
        (df.vest4Rankscore >= 0.9)

    # low and high confidence loftee predictor masks
    pred_loftee_lc = (df.impact == 'HIGH') & (df.lof == 'LC') & (df.maf < 0.01)
    pred_loftee_hc = (df.lof == 'HC')

    # create a frame for each predictor masks
    df_0of5 = df.filter(pred_0of5 | pred_5of5 | pred_loftee_hc | pred_loftee_lc)
    df_1of5 = df.filter(pred_1of5 | pred_5of5 | pred_loftee_hc | pred_loftee_lc)
    df_5of5_lc = df.filter(pred_5of5 | pred_loftee_hc | pred_loftee_lc)
    df_5of5 = df.filter(pred_5of5 | pred_loftee_hc)
    df_11of11 = df.filter(pred_11of11 | pred_loftee_hc)
    df_16of16 = df.filter(pred_16of16 | pred_loftee_hc)
    df_lof_hc = df.filter(pred_loftee_hc)

    # union all the frames together, each with their own mask id
    df = df_0of5.withColumn('burdenBinId', lit('0of5_1pct')) \
        .union(df_1of5.withColumn('burdenBinId', lit('1of5_1pct'))) \
        .union(df_5of5_lc.withColumn('burdenBinId', lit('5of5_LoF_LC'))) \
        .union(df_5of5.withColumn('burdenBinId', lit('5of5'))) \
        .union(df_11of11.withColumn('burdenBinId', lit('11of11'))) \
        .union(df_16of16.withColumn('burdenBinId', lit('16of16'))) \
        .union(df_lof_hc.withColumn('burdenBinId', lit('LoF_HC')))

    # write the frames out, each to their own folder
    df.write.mode('overwrite').json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
