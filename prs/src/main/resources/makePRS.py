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
    subprocess.run("zstdcat input/*.json.zst | jq -c '.' > input/input.json", shell=True)

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

def process_json_file(input_file, output_prefix,snp_mapping):
    max_n = 0
    file_handles = {}
    csv_writers = {} 
    processed_chromosomes = set()
    
    # Define the column names for the output files
    fieldnames = ["SNP", "A1", "A2", "BETA", "P"]

    with open(input_file, 'r') as infile:
        for line in infile:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except Exception as e:
                print(f"Error processing line: {line}\n{e}")
                continue
            
            # Update maximum sample size if needed
            n_val = record.get("n", 0)
            if n_val > max_n:
                max_n = n_val
            
            # Get the chromosome as a string
            chrom = str(record.get("chromosome"))
            
            if chrom.upper() in {"X", "Y"}:
                continue

            processed_chromosomes.add(chrom)
            
            # If this chromosome hasn't been seen before, open a new file and CSV writer
            if chrom not in file_handles:
                output_file = f"{output_prefix}.chr{chrom}.sumstats.txt"
                fh = open(output_file, 'w', newline='')
                writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter='\t')
                writer.writeheader()
                file_handles[chrom] = fh
                csv_writers[chrom] = writer
            
            # Rename and write the record for PRScsx:
            # "varId" -> "SNP", "alt" -> "A1", "reference" -> "A2",
            # "beta" -> "BETA", "pValue" -> "P", "n" -> "N"
            varid = record.get("varId")
            rsid = snp_mapping.get(varid, varid)
            p_value = record.get("pValue")
            if p_value is not None and p_value < 0.01:
                new_record = {
                    "SNP": rsid,
                    "A1": record.get("alt"),
                    "A2": record.get("reference"),
                    "BETA": record.get("beta"),
                    "P": record.get("pValue"),
                }
                csv_writers[chrom].writerow(new_record)
    
    # Close all open file handles
    for fh in file_handles.values():
        fh.close()
    
    # Return maximum n and the sorted list of chromosomes processed
    chromosomes = sorted(list(processed_chromosomes), key=lambda x: (0, int(x)) if x.isdigit() else (1, x))
    return max_n, chromosomes

def run_prscsx_by_chrom(chromosomes, ref_dir,bim_prefix, sum_stat,out_dir, out_name, n_gwas, pop, phi="1e-02"):
    n_gwas_str = str(n_gwas) if not isinstance(n_gwas, str) else n_gwas
    pop_str = pop if isinstance(pop, str) else ','.join(pop)

    for chrom in chromosomes:
        chrom_str = str(chrom)

        if chrom_str.upper() in {"X", "Y"}:
                continue
                
        sst_file = f"{sum_stat}.chr{chrom}.sumstats.txt"  # Adjust prefix if needed
        command = [
            "python3", "/mnt/var/prs/prscsx/PRScsx.py",
            f"--ref_dir={ref_dir}",
            f"--bim_prefix={bim_prefix}.{chrom}",
            f"--sst_file={sst_file}",
            f"--n_gwas={n_gwas_str}",
            f"--pop={pop_str}",
            f"--chrom={chrom_str}",
            f"--phi={phi}",
            f"--out_dir={out_dir}",
            f"--out_name={out_name}.chr{chrom}",
        ]
        print(f"Running PRScsx for chromosome {chrom} with command:\n{' '.join(command)}")
        subprocess.call(command)

def run_prscsx_for_single_chrom(chrom, ref_dir, bim_prefix, sum_stat, out_dir, out_name, n_gwas, pop, phi="1e-02"):
    """
    Build and execute the PRScsx command for a single chromosome.
    """
    chrom_str = str(chrom)
    n_gwas_str = str(n_gwas) if not isinstance(n_gwas, str) else n_gwas
    pop_str = pop if isinstance(pop, str) else ','.join(pop)
    sst_file = f"{sum_stat}.chr{chrom}.sumstats.txt"  # Adjust prefix if needed
    command = [
        "python3", "/mnt/var/prs/prscsx/PRScsx.py",
        f"--ref_dir={ref_dir}",
        f"--bim_prefix={bim_prefix}.{chrom}",
        f"--sst_file={sst_file}",
        f"--n_gwas={n_gwas_str}",
        f"--pop={pop_str}",
        f"--chrom={chrom_str}",
        f"--phi={phi}",
        f"--out_dir={out_dir}",
        f"--out_name={out_name}.chr{chrom}",
    ]
    print(f"Running PRScsx for chromosome {chrom} with command:\n{' '.join(command)}")
    subprocess.call(command)

def run_all_chromosomes_parallel(chromosomes, ref_dir, bim_prefix, sum_stat, out_dir, out_name, n_gwas, pop, phi="1e-02", max_workers=4):
    """
    Run PRScsx for all chromosomes in parallel using ProcessPoolExecutor.
    """
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for chrom in chromosomes:
            futures.append(
                executor.submit(run_prscsx_for_single_chrom,
                                chrom, ref_dir, bim_prefix, sum_stat, out_dir, out_name, n_gwas, pop, phi)
            )
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Chromosome job failed: {e}")


def combine_results(chromosomes, out_dir, out_name, combined_filename):
    combined_path = os.path.join(out_dir, combined_filename)
    column_names = ["chrom", "SNP", "position", "alt", "ref", "posterior_effect"]
    with open(combined_path, 'w', newline='') as outfile:
        # Write the header row once
        outfile.write("\t".join(column_names) + "\n")
        all_files = os.listdir(out_dir)
        matching_files = []
        for chrom in chromosomes:
            pattern = f"{out_name}.chr{chrom}_.*\.txt"
            regex = re.compile(pattern)
            matching_files += [file for file in all_files if regex.match(file)]
        for file_name in matching_files:
            file_path = os.path.join(out_dir, file_name)
            
            if not os.path.exists(file_path):
                print(f"Warning: {file_path} not found, skipping.")
                continue
            
            with open(file_path, 'r') as infile:
                infile.readline()
                for line in infile:
                    fields = line.strip().split()
                    chrom_val = fields[0]
                    snp = fields[1]
                    position = fields[2]
                    alt = fields[3]
                    ref = fields[4]
                    posterior_effect = fields[5]
                    outfile.write(f"{chrom_val}\t{snp}\t{position}\t{alt}\t{ref}\t{posterior_effect}\n")
            print(f"Added data from {file_path}")    
    print(f"Combined results written to {combined_path}")


def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage)
    parser.add_option("", "--phenotype", default=None)
    parser.add_option("", "--ancestry", default=None)

    (args,_) = parser.parse_args()

    pheno_path = f'{s3_in}/out/metaanalysis/bottom-line/ancestry-specific/{args.phenotype}/ancestry={args.ancestry}/'
    bfiles = '/mnt/var/prs/bfiles'
    out_path = f'{s3_out}/out/prs/staging/{args.phenotype}/ancestry={args.ancestry}' 

    make_json_files(pheno_path)

    input_full_path = os.path.abspath('input')

    json_file = f"{input_full_path}/input.json"
    var2rs_path = '/mnt/var/prs/snps.csv'
    output_prefix = f"{input_full_path}/input"
    ref_dir = "/mnt/var/prs/ref_info"
    bim_prefix = f"{bfiles}/1000G.EUR.QC"
    out_dir = f"{input_full_path}/out"
    out_name = "out"
    pop = "EUR"
    phi = "1e-02"

    # Ensure output directory exists
    os.makedirs(out_dir, exist_ok=True)
    
    # Step 1: Process the JSON file once to compute max_n and split by chromosome.
    snp_mapping = load_snp_mapping(var2rs_path)
    max_n, chromosomes = process_json_file(json_file, output_prefix,snp_mapping)
    n_gwas = str(max_n)
    print(f"Maximum sample size (n_gwas) found: {n_gwas}")
    print(f"Chromosomes processed: {chromosomes}")
    
    # Step 2: Run PRScsx for each chromosome
    run_prscsx_by_chrom(chromosomes, ref_dir, bim_prefix,output_prefix,out_dir, out_name, n_gwas, pop, phi)
    # run_all_chromosomes_parallel(chromosomes, ref_dir, bim_prefix, output_prefix, 
    #                             out_dir, out_name, n_gwas, pop, phi, 
    #                             max_workers= os.cpu_count())
    
    # Step 3: Combine the per-chromosome PRScsx results into a single file
    combined_filename = f"{out_name}.combined.txt"
    combine_results(chromosomes, out_dir, out_name, combined_filename)

    subprocess.check_call(['touch', f'{out_dir}/_SUCCESS'])   
    subprocess.check_call(['aws', 's3', 'cp', f'{out_dir}/{out_name}.combined.txt', f'{out_path}/'])
    safe_remove('input/input.json')
    shutil.rmtree('input')

if __name__ == "__main__":
    main()