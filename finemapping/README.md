# Fine Mapping Method

This method will take variant associations by ancestry, run the COJO method on the variant signals to find the filtered signal using 10MB locus size.

## Inputs
- variant associations

## Outputs
- The significant variant signal with the other variants pValues being conditionned on that variant

## Stages

These are the stages of finemapping.

### VariantFrequencyStage
For all variants, gathers their frequncies from the varianteffect/common directory; this is done as a separate stage to cache the frequency results in a simpler file data structure

### GatherVariantsStage
Joins the above frequencies with the phenotype/ancestry specific pValues for the variants; store the results by phenotype/ancestry subdirectory

### new RunCojoStage
Use the above frequency and pValues for phenotype/ancestry variant data and run the [COJO](https://cnsgenomics.com/software/gcta/#COJO) fine mapping calculation.
This will produce a single 'signal' SNP  10MB locus and will condition all other SNPs based on that signal pValue.
The command for each ancestry/phenotype combination is:

```
gcta_1.93.2beta/gcta64 --bfile <1000 genomes file for ancestry> --maf 0.005 --chr <chromosome> --cojo-file <pvalue_file_input> --cojo-wind 10000 --threads 4 --cojo-slct --out <file_output>
```

* cojo command option settings used:
  * Use default threshold p-value to declare a genome-wide significant hit of 5e-8
  * MAF filter of at least 0.005 for the varians to be used
  * Locus window size used is 10MB

### FinalResultsStage
This stage aggregates all the results into a standard json format per phenotype for use by the bioindex process


### GatherCredibleSetsStage
This stage aggregates all the results into a credible sets using the 'signal' SNP as the center of each 10MB locus and joining it with the neighboring conditioned pbalues.

