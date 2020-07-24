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

# count the number of processors (used for forking)
CPUS=$(cat /proc/cpuinfo | grep processor | wc | awk '{print $1}')

# run VEP
perl -I "$VEPDIR/Plugins/loftee-0.3-beta" "$VEPDIR/ensembl-vep/vep" \
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
    --biotype \
    --regulatory \
    --exclude_null_alleles \
    --flag_pick_allele \
    --pick_order tsl,biotype,appris,rank,ccds,canonical,length \
    --domains flags \
    --plugin LoF,loftee_path:$VEPDIR/Plugins/loftee-0.3-beta,human_ancestor_fa:$VEPDIR/fasta/GRCh37.primary_assembly.genome.fa,LoF,LoF_filter,LoF_flags \
    --plugin dbNSFP,$VEPDIR/dbNSFP/dbNSFP_hg19.gz,CADD_phred,CADD_raw_rankscore,CADD_raw,clinvar_clnsig,clinvar_golden_stars,clinvar_rs,clinvar_trait,DANN_rankscore,DANN_score,Eigen-PC-raw_rankscore,Eigen-PC-raw,Eigen-phred,Eigen-raw,FATHMM_converted_rankscore,FATHMM_pred,FATHMM_score,fathmm-MKL_coding_group,fathmm-MKL_coding_pred,fathmm-MKL_coding_rankscore,fathmm-MKL_coding_score,GenoCanyon_score_rankscore,GenoCanyon_score,GERP++_NR,GERP++_RS_rankscore,GERP++_RS,GM12878_confidence_value,GM12878_fitCons_score_rankscore,GM12878_fitCons_score,GTEx_V6p_gene,GTEx_V6p_tissue,H1-hESC_confidence_value,H1-hESC_fitCons_score_rankscore,H1-hESC_fitCons_score,HUVEC_confidence_value,HUVEC_fitCons_score_rankscore,HUVEC_fitCons_score,integrated_confidence_value,integrated_fitCons_score_rankscore,integrated_fitCons_score,Interpro_domain,LRT_converted_rankscore,LRT_Omega,LRT_pred,LRT_score,MetaLR_pred,MetaLR_rankscore,MetaLR_score,MetaSVM_pred,MetaSVM_rankscore,MetaSVM_score,MutationAssessor_pred,MutationAssessor_score_rankscore,MutationAssessor_score,MutationAssessor_UniprotID,MutationAssessor_variant,MutationTaster_AAE,MutationTaster_converted_rankscore,MutationTaster_model,MutationTaster_pred,MutationTaster_score,phastCons100way_vertebrate_rankscore,phastCons100way_vertebrate,phastCons20way_mammalian_rankscore,phastCons20way_mammalian,phyloP100way_vertebrate_rankscore,phyloP100way_vertebrate,phyloP20way_mammalian_rankscore,phyloP20way_mammalian,Polyphen2_HDIV_pred,Polyphen2_HDIV_rankscore,Polyphen2_HDIV_score,Polyphen2_HVAR_pred,Polyphen2_HVAR_rankscore,Polyphen2_HVAR_score,PROVEAN_converted_rankscore,PROVEAN_pred,PROVEAN_score,Reliability_index,SIFT_converted_rankscore,SIFT_pred,SIFT_score,SiPhy_29way_logOdds_rankscore,SiPhy_29way_logOdds,SiPhy_29way_pi,Transcript_id_VEST3,Transcript_var_VEST3,VEST3_rankscore,VEST3_score \
    --fields SYMBOL,NEAREST,LoF,LoF,LoF_filter,LoF_flags,CADD_phred,CADD_raw_rankscore,CADD_raw,clinvar_clnsig,clinvar_golden_stars,clinvar_rs,clinvar_trait,DANN_rankscore,DANN_score,Eigen-PC-raw_rankscore,Eigen-PC-raw,Eigen-phred,Eigen-raw,FATHMM_converted_rankscore,FATHMM_pred,FATHMM_score,fathmm-MKL_coding_group,fathmm-MKL_coding_pred,fathmm-MKL_coding_rankscore,fathmm-MKL_coding_score,GenoCanyon_score_rankscore,GenoCanyon_score,GERP++_NR,GERP++_RS_rankscore,GERP++_RS,GM12878_confidence_value,GM12878_fitCons_score_rankscore,GM12878_fitCons_score,GTEx_V6p_gene,GTEx_V6p_tissue,H1-hESC_confidence_value,H1-hESC_fitCons_score_rankscore,H1-hESC_fitCons_score,HUVEC_confidence_value,HUVEC_fitCons_score_rankscore,HUVEC_fitCons_score,integrated_confidence_value,integrated_fitCons_score_rankscore,integrated_fitCons_score,Interpro_domain,LRT_converted_rankscore,LRT_Omega,LRT_pred,LRT_score,MetaLR_pred,MetaLR_rankscore,MetaLR_score,MetaSVM_pred,MetaSVM_rankscore,MetaSVM_score,MutationAssessor_pred,MutationAssessor_score_rankscore,MutationAssessor_score,MutationAssessor_UniprotID,MutationAssessor_variant,MutationTaster_AAE,MutationTaster_converted_rankscore,MutationTaster_model,MutationTaster_pred,MutationTaster_score,phastCons100way_vertebrate_rankscore,phastCons100way_vertebrate,phastCons20way_mammalian_rankscore,phastCons20way_mammalian,phyloP100way_vertebrate_rankscore,phyloP100way_vertebrate,phyloP20way_mammalian_rankscore,phyloP20way_mammalian,Polyphen2_HDIV_pred,Polyphen2_HDIV_rankscore,Polyphen2_HDIV_score,Polyphen2_HVAR_pred,Polyphen2_HVAR_rankscore,Polyphen2_HVAR_score,PROVEAN_converted_rankscore,PROVEAN_pred,PROVEAN_score,Reliability_index,SIFT_converted_rankscore,SIFT_pred,SIFT_score,SiPhy_29way_logOdds_rankscore,SiPhy_29way_logOdds,SiPhy_29way_pi,Transcript_id_VEST3,Transcript_var_VEST3,VEST3_rankscore,VEST3_score,Uploaded_variation,Location,Allele,Gene,Feature,Feature_type,Consequence,cDNA_position,CDS_position,Protein_position,Amino_acids,Codons,CCDS,TSL,APPRIS,BIOTYPE,CANONICAL,HGNC,ENSP,DOMAINS,MOTIF_NAME,MOTIF_POS,HIGH_INF_POS,MOTIF_SCORE_CHANGE,SIFT,PolyPhen,Condel,IMPACT,PICK \
    -i "$PART" \
    -o "$OUTFILE"

# copy the output of VEP back to S3
aws s3 cp "$OUTFILE" "$S3DIR/effects/$OUTFILE"

# delete the input and output files; keep the cluster clean
rm "$PART"
rm "$OUTFILE"

# check for a warnings file, upload that, too and then delete it
if [ -e "$WARNINGS" ]; then
    aws s3 cp "$WARNINGS" "$S3DIR/effects/warnings/$WARNINGS"
    rm "$WARNINGS"
fi
