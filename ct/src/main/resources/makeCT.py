#!/usr/bin/python3
from optparse import OptionParser
import shutil
import subprocess
import os
import concurrent.futures
import json
import csv
import re

s3_in=os.environ['INPUT_PATH']
s3_out=os.environ['OUTPUT_PATH']

def make_json_files(directory):
    subprocess.check_call(['aws', 's3', 'cp', directory, 'input/', '--recursive'])
    subprocess.run('cat input/*.json > input.json', shell=True)
    shutil.rmtree('input')

def safe_remove(file_path):
    try:
        os.remove(file_path)
        print(f"File {file_path} successfully removed.")
    except FileNotFoundError:
        print(f"File {file_path} does not exist.")
    except PermissionError:
        print(f"Permission denied: cannot remove {file_path}.")
    except Exception as e:
        print(f"An error occurred while trying to remove {file_path}: {e}")

def load_snp_mapping(snp_file):
    mapping = {}
    with open(snp_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile,delimiter='\t')
        for row in reader:
            mapping[row['varId']] = row['dbSNP']
    return mapping


def process_json_file(input_file, output_file, snp_mapping, pvalue_threshold=5e-8):
    column_names = ["credibleSetId", "chrom", "SNP", "position", "alt", "ref", "beta", "stdErr", "pValue", "posterior_effect"]
    
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=column_names, delimiter='\t')
        writer.writeheader()
        
        for line in infile:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except Exception as e:
                print(f"Error processing line: {line}\n{e}")
                continue
            
            pval = record.get("pValue")
            if pval is None or pval >= pvalue_threshold:
                continue
            
            varid = record.get("varId")
            rsid = snp_mapping.get(varid, varid)

            writer.writerow({
                "credibleSetId": record.get("credibleSetId"),
                "varId": varid,
                "chrom": str(record.get("chromosome")),
                "SNP": rsid,
                "position": record.get("position"),
                "alt": record.get("alt"),
                "ref": record.get("reference"),
                "beta": record.get("beta"),
                "stdErr": record.get("stdErr"),
                "pValue": pval,
                "posterior_effect": record.get("posteriorProbability"),
            })


def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage)
    parser.add_option("", "--phenotype", default=None)
    parser.add_option("", "--ancestry", default=None)

    (args,_) = parser.parse_args()

    pheno_path = f'{s3_in}/out/credible_sets/intake/{args.phenotype}/{args.ancestry}/bottom-line/'
    out_path = f'{s3_out}/out/ct/staging/{args.phenotype}/ancestry={args.ancestry}/bottom-line' 

    make_json_files(pheno_path)

    json_file = "input.json"
    var2rs_path = '/mnt/var/prs/snps.csv'
    out_dir = "out"

    # Ensure output directory exists
    os.makedirs(out_dir, exist_ok=True)
    
    snp_mapping = load_snp_mapping(var2rs_path)
    process_json_file(json_file, f"{out_dir}/C_T.txt",snp_mapping)
    
    subprocess.check_call(['touch', f'{out_dir}/_SUCCESS'])
    subprocess.check_call(['aws', 's3', 'cp', f'{out_dir}/', out_path,'--recursive'])
    safe_remove('input.json')
    shutil.rmtree(out_dir)

if __name__ == "__main__":
    main()