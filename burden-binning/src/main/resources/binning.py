import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode, lit, when, input_file_name, udf, split


CQS_SRCDIR = 's3://dig-analysis-data/out/varianteffect/ld_server/cqs'
VARIANTS_SRCDIR = 's3://dig-analysis-data/ld_server/variants'


def load_vep_consequences(spark):
    """
    Loads the VEP data, explodes consequences
    """
    df = spark.read.json(f'{CQS_SRCDIR}/part-*')

    df.na.fill(value="", subset=["hgvsp"])
    df = df \
        .withColumn('proteinChange', split(df['hgvsp'], ':').getItem(1)) \
        .drop('hgvsp')

    # only keep specific fields
    return df.select(
        df.varId,
        df.geneId,
        df.geneSymbol,
        df.transcriptId,
        df.pick,
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
        df.consequenceTerms,
        df.proteinChange
    )


def load_unique_variants(spark, datatype):
    """
    Only load variants from specific tech datasets.
    """

    # Need to get the datatype into the data for use in bioindex
    # Look at how we do this with the regex thing in ldsc
    srcdir = f'{VARIANTS_SRCDIR}/{datatype}/part-*'

    # get just the list of unique variants
    df = spark.read.json(srcdir) \
        .select('varId')

    # only unique variants
    return df.dropDuplicates(['varId'])


def main():
    """
    Arguments: datatype
    """
    opts = argparse.ArgumentParser()
    opts.add_argument('--datatype', type=str, required=True)
    args = opts.parse_args()

    spark = SparkSession.builder.appName('burdenbinning').getOrCreate()

    # where to output the results to
    gene_outdir = f's3://dig-analysis-data/out/burdenbinning/gene/{args.datatype}'
    transcript_outdir = f's3://dig-analysis-data/out/burdenbinning/transcript/{args.datatype}'

    # load input data
    variants = load_unique_variants(spark, args.datatype)
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

    df = df.withColumn('datatype', lit(args.datatype))
    gene_df = df \
        .filter(df.pick == 1) \
        .drop('pick')
    transcript_df = df \
        .drop('pick')

    # write the frames out, each to their own folder
    gene_df.write.mode('overwrite').json(gene_outdir)
    transcript_df.write.mode('overwrite').json(transcript_outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
