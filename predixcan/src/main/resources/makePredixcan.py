#!/usr/bin/python3
from optparse import OptionParser
import shutil
import subprocess
import os


s3_in=os.environ['INPUT_PATH']
s3_out=os.environ['OUTPUT_PATH']

# def finds json files in the directory
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
    fieldnames = ["SNP", "A1", "A2", "BETA", "SE","P"]

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
            
            # if chrom.upper() in {"X", "Y"}:
            #     continue

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
                    "SE": record.get("stdErr"),
                    "P": record.get("pValue"),
                }
                csv_writers[chrom].writerow(new_record)
    
    # Close all open file handles
    for fh in file_handles.values():
        fh.close()
    
    # Return maximum n and the sorted list of chromosomes processed
    chromosomes = sorted(list(processed_chromosomes), key=lambda x: (0, int(x)) if x.isdigit() else (1, x))
    return max_n, chromosomes

def main():
	usage = "usage: %prog [options]"
	parser = OptionParser(usage)
	parser.add_option("", "--phenotype", default=None)
	parser.add_option("", "--ancestry", default=None)

	(args,_) = parser.parse_args()

	pheno_path = f'{s3_in}/out/metaanalysis/bottom-line/ancestry-specific/{args.phenotype}/ancestry={args.ancestry}/'
	var2rs_path = '/mnt/var/predixcan/snps.csv'
	out_path = f'{s3_out}/out/predixcan/staging/{args.phenotype}/ancestry={args.ancestry}'

	# read all files in the clump path
	make_json_files(pheno_path)

    input_full_path = os.path.abspath('input')
    output_prefix = f"{input_full_path}/input"

    json_file = f"{input_full_path}/input.json"

    snp_mapping = load_snp_mapping(var2rs_path)
    max_n, chromosomes = process_json_file(json_file,output_prefix,snp_mapping)
    
	# create the tmp out directory
	out_directory = 'data'
	if not os.path.exists(out_directory):
		os.makedirs(out_directory, exist_ok=True)
	
	out_directory_full_path = os.path.abspath(out_directory)

    # path to your MetaXcan software dir
    spredixcan_dir = '~/MetaXcan-master/software'

    subprocess.call([
        'bash', f'{spredixcan_dir}/SPrediXcan.py',
        '--model_db_path',      f'{input_full_path}/model_db_cov_files/l-ctPred_celli.db',
        '--covariance',         f'{input_full_path}/model_db_cov_files/covariance.txt.gz',
        '--gwas_folder',        input_full_path,
        '--gwas_file_pattern',  '.*txt',
        '--snp_column',         'SNP',
        '--effect_allele_column','A1',
        '--non_effect_allele_column','A2',
        '--beta_column',        'BETA',
        '--pvalue_column',      'P',
        '--output_file',        f'{out_directory_full_path}/TWAS_result.csv'
    ])


	subprocess.check_call(['touch', f'{out_directory}/_SUCCESS'])	
	subprocess.check_call(['aws', 's3', 'cp', f'{out_directory}/', out_path, '--recursive'])
	safe_remove('input/input.json')
	shutil.rmtree('input')
	shutil.rmtree(out_directory)

if __name__ == '__main__':
	main()
