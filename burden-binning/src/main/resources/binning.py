import functools

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode, lit


VEP_SRCDIR = 's3://dig-analysis-data/out/varianteffect/effects'
VARIANTS_SRCDIR = 's3://dig-analysis-data/variants'
TECH_SOURCES = ['ExSeq', 'WGS']


def load_vep(spark):
    """
    Loads the VEP data, explodes consequences, and picks the best one.
    """
    vep = spark.read.json(f'{VEP_SRCDIR}/part-*')
    cqs = explode(vep.transcript_consequences)

    # mapping of varId -> consequence; filter the picked consequences
    df = vep.select(vep.id, cqs.alias('cqs'))
    df = df.filter(df.cqs.pick == 1)

    return df.select(
        df.id.alias('varId'),

        # pivot the cqs map into columns
        df.cqs['gene_id'].alias('ensemblId'),
        df.cqs['gene_symbol'].alias('gene'),
        df.cqs['transcript_id'].alias('transcriptId'),
        df.cqs['lof'].alias('lof'),
        df.cqs['impact'].alias('impact'),
        df.cqs['pick'].alias('pick'),
        df.cqs['polyphen2_hdiv_pred'].alias('polyphen2_hdiv_pred'),
        df.cqs['polyphen2_hvar_pred'].alias('polyphen2_hvar_pred'),
        df.cqs['sift_pred'].alias('sift_pred'),
        df.cqs['mutationtaster_pred'].alias('mutationtaster_pred'),
        df.cqs['lrt_pred'].alias('lrt_pred'),
        df.cqs['metalr_pred'].alias('metalr_pred'),
        df.cqs['provean_pred'].alias('provean_pred'),
        df.cqs['fathmm_pred'].alias('fathmm_pred'),
        df.cqs['fathmm-mkl_coding_pred'].alias('fathmm_mkl_coding_pred'),
        df.cqs['eigen-pc-raw_coding_rankscore'].alias('eigen_pc_raw_coding_rankscore'),
        df.cqs['dann_rankscore'].alias('dann_rankscore'),
        df.cqs['vest4_rankscore'].alias('vest4_rankscore'),
        df.cqs['cadd_raw_rankscore'].alias('cadd_raw_rankscore'),
        df.cqs['metasvm_pred'].alias('metasvm_pred'),
        df.cqs['gnomad_genomes_popmax_af'].alias('gnomad_genomes_popmax_af'),
    )


def load_variants(spark):
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
    variants = load_variants(spark)
    vep = load_vep(spark)

    # keep only the vep output for the variants we care about
    df = variants.join(vep, on='varId', how='inner')

    # 0/5 predictor masks
    pred_0of5 = (df.impact == 'MODERATE') & (df.gnomad_genomes_popmax_af < 0.01)

    # 1/5 predictor masks
    pred_1of5 = pred_0of5 & (
        (df.polyphen2_hdiv_pred.contains('D')) | \
        (df.polyphen2_hvar_pred.contains('D')) | \
        (df.sift_pred.contains('D')) | \
        (df.lrt_pred.contains('D')) | \
        (df.mutationtaster_pred.contains('A') | df.mutationtaster_pred.contains('D')) \
    )

    # 5/5 predictor masks
    pred_5of5 = \
        (df.polyphen2_hdiv_pred.contains('D')) & \
        (df.polyphen2_hvar_pred.contains('D')) & \
        (df.sift_pred.contains('D')) & \
        (df.lrt_pred.contains('D')) & \
        (df.mutationtaster_pred.contains('A') | df.mutationtaster_pred.contains('D'))

    # 5/5 + 6 more masks
    pred_11of11 = pred_5of5 & \
        (df.metalr_pred.contains('D')) & \
        (df.metasvm_pred.contains('D')) &  \
        (df.provean_pred.contains('D')) & \
        (df.fathmm_mkl_coding_pred.contains('D')) & \
        (df.fathmm_pred.contains('D'))

    # NOTE: 11/11 needs `& (M-CAP_score >= 0.025)`, but needs a newer dbNSFP

    # 11/11 + 4 more masks
    pred_16of16 = pred_11of11 & \
        (df.eigen_pc_raw_coding_rankscore >= 0.9) & \
        (df.dann_rankscore >= 0.9) & \
        (df.cadd_raw_rankscore >= 0.9) & \
        (df.vest4_rankscore >= 0.9)

    # low and high confidence loftee predictor masks
    pred_loftee_lc = (df.impact == 'HIGH') & (df.lof == 'LC') & (df.gnomad_genomes_popmax_af < 0.01)
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
