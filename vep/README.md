# Variant Effect Pipeline

This pipeline uses VEP to discover the transcript consequences and regulatory features of all variants across all datasets.

## Listing Variants

The first step uses Spark to find all the distinct, bi-allelic variants across all datasets and output them in the [default format][format] used by VEP.

## Running VEP

Next, all the part files produced in the list step are parallelized across multiple clusters. Each step on each cluster will run multiple instances of VEP at the same time.

For example, if there are 5000 part files produced by the list step, there may be 10 clusters created, each with 25 steps, where each step will run VEP 20 times in parallel over a given part file.

The output of this is the `--json` format, which is copied back to HDFS to be read directly by Spark in the next step.

## Exploding Consequences

Finally, each of the output `json` files produced by running VEP are loaded into Spark and the transcript consequences and regulatory features for each variant are exploded and written back to HDFS as CSV files.

For example, if a single variant has 3 transcript consequences and 2 regulatory features, then in the output for transcript consequences there will be 3 rows and in the output for regulatory features there will be 2 rows.

## Loading Consequences

The final transcript consequence and regulatory feature CSVs are loaded into the graph database.


[format]: https://useast.ensembl.org/info/docs/tools/vep/vep_formats.html
