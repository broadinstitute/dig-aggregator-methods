# magma

The Magma process uses the bottom line pValue results for all the phenotypes to generate corresponding gene pValue files per phenotype.

## Stages

These are the stages of magma.

### Step 1 - gather the rsIDs

PySpark script to pull from the common gene data store and generate the input file for the next steps.
The file with rows corresponding to SNPs; a header is not allowed.
The file must have three columns containing the SNP ID, chromosome and base pair position, in that
order. Allowed chromosome codes are 1-24, X and Y (where 23 and 24 correspond to X and Y
respectively).

### Step 2 - assign the common variants to genes

Shell script that calls the magma command to assign variants to genes; the generated assignment file will be used later 
generate gene pValues based on each gene's assigned varnats and their corresponding pValues.

The shell runs the command:
magma --annotate --snp-loc <file from step1> --gene-loc <reference file from magma site> --out <output>

### Step 3 - gather the common variants pValues for all the phenotypes

PySpark script to generate a file for each phenotype in the aggregator; the file will contain all the common variants with rsIDs and their pvalues and the sample numbers used to calculate the pValues.
The resulting p-value file must be a plain text data file with each row corresponding to a SNP. If MAGMA
detects a header in the file it will look for SNP IDs and p-values in the SNP and P column respectively. 

### Step 4 - calculate the gene pValues

Shell script that generates the gene pValues based on the stes2 and step3 files.

The shell runs the command:
magma --bfile <1000 genome file> --pval <step3 file> --gene-annot <step2 file> --out <output file>

### Step 5 - convert the magma gene pValue files to aggregator json files

PySpark script to pull the data from the step4 magma process file and join with the reference phenotype ontology 
id file. Saves anh individual json file for each phenotype with a gene/pValue combination per row.

