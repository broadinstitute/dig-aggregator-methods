# Meta-Analysis Pipeline

This document describes the steps taken to perform meta-analysis for a given phenotype across all datasets.

## Partitioning Variants

For each dataset, the variants are first filtered and then partitioned by:

1. Ancestry
2. MAF (common/rare)

In the filter step, the following variants are removed from the meta-analysis:

* Multi-allelics variants
* Variants with missing p-value or beta/OR

## Running Meta-Analysis

The meta-analysis is broken up into 2 steps: ancestry-specific and trans-ethnic.

### Ancestry-Specific Analysis

If more than one ancestry is present for the phenotype (e.g. EU, HS, and Mixed), then - if present - the "Mixed" ancestry is removed from further analysis. 

Then, for each ancestry, the following analysis is performed:

1. METAL is run over common variants from each dataset with `OVERLAP ON`.
2. The output is then unioned with the rare variants from all datasets.
3. In the event that a variant exists as both common and rare (from differing datasets), only the variant with the largest, total `N` across all datasets is kept.

### Trans-Ethnic Analysis

After each ancestry has been processed, METAL is run across all the ancestries with `OVERLAP OFF`. If the only ancestry present was "Mixed" then this should result in an identity operation.

## Loading Results

Only the final results of the trans-ethnic analysis are loaded into the database as "bottom line" results.
