import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

FORESTPLOT_SCHEMA = StructType([
    StructField('row_kind', StringType()),
    StructField('gene_key', StringType()),
    StructField('gene', StringType()),
    StructField('standardized_outcome_id', StringType()),
    StructField('standardized_outcome_label', StringType()),
    StructField('outcome_type', StringType()),
    StructField('effect_metric', StringType()),
    StructField('effect_size', DoubleType()),
    StructField('ci_low', DoubleType()),
    StructField('ci_high', DoubleType()),
    StructField('p_value', DoubleType()),
    StructField('p_value_adj', DoubleType()),
    StructField('datasetId', StringType()),
    StructField('datasetName', StringType()),
    StructField('dataset_dir', StringType()),
    StructField('dataset_family', StringType()),
    StructField('modality', StringType()),
    StructField('species', StringType()),
    StructField('tissue', StringType()),
    StructField('depot', StringType()),
    StructField('depot2', StringType()),
    StructField('level_a', StringType()),
    StructField('level_b', StringType()),
    StructField('direction_label', StringType()),
    StructField('source_level_a', StringType()),
    StructField('source_level_b', StringType()),
    StructField('source_effect_size', DoubleType()),
    StructField('effect_definition', StringType()),
    StructField('positive_direction_text', StringType()),
    StructField('negative_direction_text', StringType()),
    StructField('direction_standardized', StringType()),
    StructField('direction_notes', StringType()),
    StructField('n_a', DoubleType()),
    StructField('n_b', DoubleType()),
    StructField('n', DoubleType()),
    StructField('mean_a', DoubleType()),
    StructField('mean_b', DoubleType()),
    StructField('sd_a', DoubleType()),
    StructField('sd_b', DoubleType()),
    StructField('original_metadata_field', StringType()),
    StructField('meta_scope', StringType()),
    StructField('meta_pool_id', StringType()),
    StructField('pooled_group_label', StringType()),
    StructField('supporting_datasets', StringType()),
    StructField('supporting_gene_symbols', StringType()),
    StructField('k_datasets', DoubleType()),
    StructField('fixed_effect', DoubleType()),
    StructField('fixed_ci_low', DoubleType()),
    StructField('fixed_ci_high', DoubleType()),
    StructField('random_effect', DoubleType()),
    StructField('random_ci_low', DoubleType()),
    StructField('random_ci_high', DoubleType()),
    StructField('random_p_value', DoubleType()),
    StructField('Q', DoubleType()),
    StructField('I2', DoubleType()),
    StructField('tau2', DoubleType()),
    StructField('notes', StringType()),
])


def main():
    """
    Arguments: none
    """
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    srcdir = f'{s3_in}/cross_dataset_test/all_gene_bulk_forestplot_endpoint_handoff.tsv'
    outdir = f'{s3_bioindex}/single_cell/forest'

    df = spark.read.csv(srcdir, sep='\t', header=True, schema=FORESTPLOT_SCHEMA)

    # sort and write
    df.orderBy(['gene_key', 'row_kind', 'p_value']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
