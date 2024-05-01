import os
from pyspark.sql import SparkSession

s3_out = os.environ['OUTPUT_PATH']


def main():
    genes_dir = f's3://dig-analysis-bin/genes/GRCh37'
    out_dir = f'{s3_out}/out/huge/geneidmap/genes/'

    spark = SparkSession.builder.appName('gene_id_map').getOrCreate()
    genes = spark.read.json(f'{genes_dir}/part-*')\
        .select("chromosome", "start", "end", "name", "source")

    genes_symbol = genes \
        .filter(genes.source == "symbol") \
        .select("chromosome", "start", "end", "name") \
        .withColumnRenamed("name", "symbol")
    genes_ensembl = genes \
        .filter(genes.source == "ensembl") \
        .select("chromosome", "start", "end", "name") \
        .withColumnRenamed("name", "ensembl")
    genes_joined = genes_symbol.join(genes_ensembl, ["chromosome", "start", "end"])

    genes_joined.write \
        .mode("overwrite") \
        .json(out_dir)
    spark.stop()


if __name__ == '__main__':
    main()
