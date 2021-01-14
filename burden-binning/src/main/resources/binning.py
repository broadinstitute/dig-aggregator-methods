from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode, lit


VEP_SRCDIR = 's3://dig-analysis-data/out/varianteffect/effects'
FREQ_SRCDIR = 's3://dig-analysis-data/out/frequencyanalysis'


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
        df.cqs['eigen-pc-raw_rankscore'].alias('eigen_pc_raw_rankscore'),
        df.cqs['dann_rankscore'].alias('dann_rankscore'),
        df.cqs['vest3_rankscore'].alias('vest3_rankscore'),
        df.cqs['cadd_raw_rankscore'].alias('cadd_raw_rankscore'),
        df.cqs['metasvm_pred'].alias('metasvm_pred'),
    )


def load_freq(spark):
    """
    Loads the frequency analysis data.
    """
    df = spark.read.json(f'{FREQ_SRCDIR}/*/part-*')

    # remove null entries and map just variant -> maf
    df = df.filter(df.maf.isNotNull())
    df = df.select(df.varId, df.maf)

    # select the maximum freq across all ancestries
    return df.rdd.reduceByKey(max) \
        .map(lambda r: Row(varId=r[0], maf=r[1])) \
        .toDF()


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('burdenbinning').getOrCreate()

    # where to output the results to
    outdir = 's3://dig-analysis-data/out/burdenbinning'

    # load input data
    vep = load_vep(spark)
    freq = load_freq(spark)

    # join MAF with the transcript consequence
    df = vep.join(freq, on='varId')

    # 0/5 predictor masks
    pred_0of5 = (df.impact == 'MODERATE') & (df.maf < 0.01)

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
        (df.eigen_pc_raw_rankscore >= 0.9) & \
        (df.dann_rankscore >= 0.9) & \
        (df.cadd_raw_rankscore >= 0.9) & \
        (df.vest3_rankscore >= 0.9)

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
