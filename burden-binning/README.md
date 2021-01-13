# Burden Binning

The purden binning process is a PySpark script that loads all variants/gene transcript from the vep results bucket, joins with the frequency analysis results to get MAF values and then filters the variants into 7 bins (1 through 7, 1 being the most restrictive); the bins build on each other, so bin1 is contained in bin2 which is contained in bin3 etc.

The filters for the bins are described in the PDF file contained in the docs directory

The unioned 7 bins are then written out to disk after being sorted by gene and bin number.

## Stages

### BurdenbinningStage

The only stage for this process is the PySpark process described above
