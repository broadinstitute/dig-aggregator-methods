#!/bin/bash -xe

# set where the source and destination is in S3 and where VEP is
S3DIR="s3://dig-analysis-data/out/varianteffect"
VEPDIR="/mnt/var/vep"

# get the name of the part file from the command line; set the output filename
PART=$(basename -- "$1")
OUTFILE="${PART%.*}.json"
WARNINGS="${OUTFILE}_warnings.txt"

# update the path to include samtools and tabix
PATH="$PATH:$VEPDIR/samtools-1.9/:$VEPDIR/ensembl-vep/htslib"

# copy the part file from S3 to local
aws s3 cp "$S3DIR/variants/$PART" .
extension="${PART##*.}"
if [ "$extension" = "zst" ]
then
  unzstd "$PART"
  rm "$PART"
  PART="${PART%.*}"
fi

# ensure the file is sorted
sort -k1,1 -k2,2n "$PART" > "$PART.sorted"

# count the number of processors (used for forking)
CPUS=$(cat /proc/cpuinfo | grep processor | wc | awk '{print $1}')

# run VEP
perl -I "$VEPDIR/loftee-0.3-beta" "$VEPDIR/ensembl-vep/vep" \
    --dir "$VEPDIR" \
    --fork "$CPUS" \
    --format ensembl \
    --buffer_size 15000 \
    --json \
    --offline \
    --no_stats \
    --fasta "$VEPDIR/fasta/GRCh37.primary_assembly.genome.fa" \
    --polyphen b \
    --sift b \
    --ccds \
    --nearest symbol \
    --symbol \
    --appris \
    --tsl \
    --hgvs \
    --biotype \
    --regulatory \
    --exclude_null_alleles \
    --flag_pick_allele \
    --pick_order tsl,biotype,appris,rank,ccds,canonical,length \
    --domains flags \
    --af_1kg \
    --max_af \
    --plugin LoF,loftee_path:$VEPDIR/loftee-0.3-beta,human_ancestor_fa:$VEPDIR/fasta/GRCh37.primary_assembly.genome.fa,LoF,LoF_filter,LoF_flags \
    --plugin dbNSFP,$VEPDIR/dbNSFP/dbNSFP4.1a_grch37.gz,M-CAP_score,CADD_raw_rankscore,DANN_rankscore,Eigen-PC-raw_coding_rankscore,Polyphen2_HDIV_pred,Polyphen2_HVAR_pred,SIFT_pred,LRT_pred,MutationTaster_pred,FATHMM_pred,fathmm-MKL_coding_pred,PROVEAN_pred,MetaSVM_pred,MetaLR_pred,VEST4_score,VEST4_rankscore,gnomAD_genomes_POPMAX_AF \
    --fields SYMBOL,NEAREST,IMPACT,MAX_AF,AFR_AF,AMR_AF,EAS_AF,EUR_AF,SAS_AF,HGVSc,HGVSp,HGVS_OFFSET,PICK,CCDS,TSL,APPRIS,BIOTYPE,CANONICAL,HGNC,ENSP,DOMAINS,MOTIF_NAME,MOTIF_POS,HIGH_INF_POS,MOTIF_SCORE_CHANGE,SIFT,cDNA_position,CDS_position,Amino_acids,Codons,Protein_position,Protein_change,LoF,LoF_filter,LoF_flags,M-CAP_score,CADD_raw_rankscore,DANN_rankscore,Eigen-PC-raw_coding_rankscore,Polyphen2_HDIV_pred,Polyphen2_HVAR_pred,SIFT_pred,LRT_pred,MutationTaster_pred,FATHMM_pred,fathmm-MKL_coding_pred,PROVEAN_pred,MetaSVM_pred,MetaLR_pred,VEST4_score,VEST4_rankscore,gnomAD_genomes_POPMAX_AF \
    -i "$PART.sorted" \
    -o "$OUTFILE" \
    --force_overwrite

# copy the output of VEP back to S3
aws s3 cp "$OUTFILE" "$S3DIR/effects/$OUTFILE"

# delete the input and output files; keep the cluster clean
rm "$PART"
rm "$PART.sorted"
rm "$OUTFILE"

# check for a warnings file, upload that, too and then delete it
if [ -e "$WARNINGS" ]; then
    aws s3 cp "$WARNINGS" "$S3DIR/warnings/$WARNINGS"
    rm "$WARNINGS"
fi
