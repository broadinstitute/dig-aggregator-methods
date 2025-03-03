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

def main():
	usage = "usage: %prog [options]"
	parser = OptionParser(usage)
	parser.add_option("", "--phenotype", default=None)
	parser.add_option("", "--ancestry", default=None)

	(args,_) = parser.parse_args()

	pheno_path = f'{s3_in}/out/metaanalysis/largest/ancestry-specific/{args.phenotype}/ancestry={args.ancestry}/'
	var2rs_path = '/mnt/var/cojo/snps.csv'
	bfiles = '/mnt/var/cojo/bfiles'
	finemap_dir = '/mnt/var/cojo/finemapping'
	config_file = f'{finemap_dir}/only_cojo.config.yaml'
	out_path = f'{s3_out}/out/cojo/staging/{args.phenotype}/ancestry={args.ancestry}' 

	# read all files in the clump path
	make_json_files(pheno_path)

	# create the tmp out directory
	out_directory = 'data'
	if not os.path.exists(out_directory):
		os.makedirs(out_directory, exist_ok=True)
	
	out_directory_full_path = os.path.abspath(out_directory)
	input_full_path = os.path.abspath('input')

	subprocess.call(['bash', f'{finemap_dir}/run_finemap_pipeline.sh', 
					'--input',input_full_path,
					'--bfiles', bfiles,
					'--config_file',config_file,
					'--dbsnp_file',var2rs_path,
					'--output', out_directory_full_path,
					'--finemap_dir',finemap_dir
					])
	
	subprocess.check_call(['touch', f'{out_directory}/_SUCCESS'])	
	subprocess.check_call(['aws', 's3', 'cp', f'{out_directory}/', out_path, '--recursive'])
	safe_remove('input/input.json')
	shutil.rmtree('input')
	shutil.rmtree(out_directory)

if __name__ == '__main__':
	main()
