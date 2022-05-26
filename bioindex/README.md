# Bio Index

This is the final method run over all the aggregated data. It copies it, joined and sorted into the `dig-bio-index` S3 bucket so that it may be indexed by the BioIndex.

## Stages

This describes each stage and the output produced by it. Regardless of stage, the outputs of the BioIndex are very consistent. The bucket is formatted like so:

```
s3://dig-bio-index/<output type>/<sort key>/...
```

Where the `<output type>` would be one of:

* associations
* credible_sets
* gene_associations
* genes
* plots
* regions
* transcription_factors
* variant
* ...

The `<sort key>` is the first key of the sort index or "locus" if only sorted by locus. If an index was to return associations by phenotype and locus, the output of that index would be written to:

```
s3://dig-bio-index/associations/phenotype/...
```

There may be other subfolders under it (e.g. each phenotype), but indexing would happen at the root directory in S3 for that index.

### AnnotatedRegionsStage

Joins all annotated regions with the tissue ontology then sorts them by annotation and then region before writing it to `regions/annotation/...`.

The output of this stage is queried with the `annotated-regions` index.

### AssociationsStage

Joins the results of the bottom-line method with the common variants of the VEP method into `associations/phenotype/<phenotype>/...`. The records are sorted by phenotype and then by position.

The output of this stage is queried with the `associations` index.

### CredibleSetsStage

Several operations and sets of data are generated:

* Credible set IDs are grouped together and sorted by region and written to `credible_sets/locus/...`.
* All the credible set variants are grouped together by credible set ID and written to `credible_sets/variants/...`.
* Annotated regions are loaded, the most significant regions overlapping the variants of each credible set are sorted by credible set ID and written to `credible_sets/regions/...`

The credible set IDs can be queried using the `credible-sets` index.

The variants of a credible set can be queried using the `credible-variants` index.

The regions overlapping a credible set can be queried using the `credible-regions` index.

### DatasetAssociationsStage

Joins the common variants of the VEP method with the "top" variants of each dataset. The "top" variants are the top either the 500 most significant or all the significant variants (whichever is more) from the dataset. It also runs the plotting code to produce the manhattan and qq plots for each dataset.

The output if this stage is queried with the `dataset-associations` index.

### EffectorGenesStage

TODO:

### GeneAssociationsStage

Loads all the gene association EPACTS datasets and sorts them by gene and then pValue and written to `gene_associations/...`.

_NOTE: Right now, this is special for the 52k as it is the only EPACTS gene associations dataset we have, and is queried using the `gene-associations-52k` index._

The MAGMA results are also loaded and sorted/written to two different locations:

* Sorted by gene, pValue and written to `gene_associations/gene/...`.
* Sorted by phenotype, pValue and written to `finder/gene/...`.

The `gene-associations` index can be used to lookup the phenotypes associated with a gene.

The `gene-finder` index can be used to lookup genes associated with a phenotype.

### GenesStage

Sorts the genes in the aggregator by position and writing it to `genes/...`. However, it is indexed by both the `gene` index (by name) and the `genes` index (by position).

### GlobalEnrichmentStage

Joins the output of the GREGOR method with the tissue ontology data, sorts it by phenotype and then pValue (so a query with `limit=1` will return the most impacted tissue) and writes it to `global_enrichment/phenotype/...`.

The output of this stage is queried with the `global-enrichment` index.

### GlobalAssociationsStage

The results of the bottom-line analysis are loaded - per phenotype - and all globally significant (or top 500) associations are kept per phenotype. The output is sorted by pValue and written to the `associations/global/...` directory.

The output of this stage is queried with the `global-associations` index.

### PhewasAssociationsStage

The results of the bottom-line analysis are loaded, joined to the clump ids, sorted by pValue, and grouped by variant ID before being written to `associations/phewas/...`.

The output of this stage is queried with the `phewas-associations` index.

### TopAssociationsStage

This stage takes the bottom line analysis across all phenotypes, joined with the common data from VEP, and finds the best association - per phenotype - every 50 kb across the genome and then sorted by position.

The output of this stage is queried with the `top-associations` index.

### TranscriptionsStage

Sorts all the transcription factors loaded by variant ID and writes them to `transcription_factors/...`.

The output of this stage is queried with the `transcription-factors` index.

### VariantAssociationsStage

Takes all the GWAS datasets together, sorts them by variant ID, and written to `associations/variant/...`.

The output of this stage is queried with the `variant-dataset-associations` index.

### VariantsStage

The variants stage simply copies the output of the common data from VEP to the `varaints/...` directory. It isn't sorted, and is indexed by both variant ID and dbSNP (rsID).

The output of this stage is queried with the `variant` index.
