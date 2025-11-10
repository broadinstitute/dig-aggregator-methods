import glob
import json
import numpy as np
import os
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, DoubleType
from pyspark.sql.functions import udf, when, lit

s3_in = os.environ['INPUT_PATH']
s3_bioindex = os.environ['BIOINDEX_PATH']

GENES_SCHEMA = StructType(
    [
        StructField('datasetId', StringType()),
        StructField('gene', StringType()),
        StructField('cell_type__kp', StringType()),
        StructField('mean_expression_raw', DoubleType()),
        StructField('mean_expression_scaled', DoubleType()),
        StructField('pct_cells_expression', DoubleType()),
        StructField('log_fold_change', DoubleType()),
        StructField('p_value', DoubleType()),
        StructField('p_value_adj', DoubleType())
    ]
)

BULK_SCHEMA = StructType(
    [
        StructField('datasetId', StringType()),
        StructField('dea_comp_field', StringType()),
        StructField('dea_comp_name', StringType()),
        StructField('dea_comp_id', StringType()),
        StructField('Gene', StringType()),
        StructField('log_fold_change', DoubleType()),
        StructField('abs(log_fold_change)', DoubleType()),
        StructField('log_cpm', DoubleType()),
        StructField('stat', DoubleType()),
        StructField('p_value', DoubleType()),
        StructField('p_value_adj', DoubleType()),
        StructField('-log10(p_value)', DoubleType()),
        StructField('-log10(p_value_adj)', DoubleType()),
        StructField('gene_label', StringType()),
    ]
)

gene_to_upper = udf(lambda s: s.upper())

def get_dataset_to_species():
    dataset_to_species = {}
    subprocess.check_call(f'aws s3 cp {s3_in}/single_cell/ metadata/ --recursive --exclude="*" --include="*dataset_metadata.json"', shell=True)
    subprocess.check_call(f'aws s3 cp {s3_in}/bulk_rna/ metadata/ --recursive --exclude="*" --include="*dataset_metadata.json"', shell=True)
    for file in glob.glob('metadata/*/dataset_metadata.json'):
        with open(file, 'r') as f:
            metadata = json.load(f)
        dataset_to_species[metadata['datasetId']] = metadata['species']
    return udf(lambda s: dataset_to_species[s])


def get_genes(spark, dataset_to_species):
    srcdir = f'{s3_in}/single_cell/*/marker_genes.tsv'
    df = spark.read.csv(srcdir, header=True, sep='\t', schema=GENES_SCHEMA)
    df = df.withColumnRenamed('cell_type__kp', 'datasetRef')
    df = df.withColumn('gene', gene_to_upper(df.gene))
    df = df.withColumn('species', dataset_to_species(df.datasetId))
    df = df.withColumn('datasetType', lit('single_cell'))
    df = df.withColumn('p_value', when(df.p_value == 0.0, np.nextafter(0, 1)).otherwise(df.p_value))
    df = df.withColumn('p_value_adj', when(df.p_value_adj == 0.0, np.nextafter(0, 1)).otherwise(df.p_value_adj))
    df = df[df.p_value_adj < 0.05]
    return df[['gene', 'species', 'datasetType', 'datasetId', 'datasetRef', 'log_fold_change', 'p_value', 'p_value_adj']]


def get_bulk(spark, dataset_to_species):
    srcdir = f'{s3_in}/bulk_rna/*/dea.tsv.gz'
    df = spark.read.csv(srcdir, header=True, sep='\t', schema=BULK_SCHEMA)
    df = df.withColumnRenamed('dea_comp_name', 'datasetRef')
    df = df.withColumnRenamed('Gene', 'gene')
    df = df.withColumn('gene', gene_to_upper(df.gene))
    df = df.withColumn('species', dataset_to_species(df.datasetId))
    df = df.withColumn('datasetType', lit('bulk_rna'))
    df = df.withColumn('p_value', when(df.p_value == 0.0, np.nextafter(0, 1)).otherwise(df.p_value))
    df = df.withColumn('p_value_adj', when(df.p_value_adj == 0.0, np.nextafter(0, 1)).otherwise(df.p_value_adj))
    df = df[df.p_value_adj < 0.05]
    return df[['gene', 'species', 'datasetType', 'datasetId', 'datasetRef', 'log_fold_change', 'p_value', 'p_value_adj']]


def main():
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    dataset_to_species = get_dataset_to_species()
    df = get_genes(spark, dataset_to_species)
    df = df.union(get_bulk(spark, dataset_to_species))
    outdir = f'{s3_bioindex}/single_cell/genes'

    # write associations sorted by variant and then pValue
    df.orderBy(['gene', 'p_value_adj']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()


if __name__ == '__main__':
    main()
