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
	# check=True: an unchecked failure here (e.g. zstdcat missing, bad glob,
	# malformed .zst) used to leave input/input.json empty/missing while the
	# script carried on regardless - the real cause of a false-success EMR
	# step (see main()).
	subprocess.run("zstdcat input/*.json.zst | jq -c '.' > input/input.json", shell=True, check=True)
	if not os.path.getsize('input/input.json'):
		raise RuntimeError("input/input.json is empty after zstdcat|jq - decompression/parsing of the .json.zst shards produced no output.")

	# Validate input.json is well-formed NDJSON right at its point of
	# creation, so a corrupt-content failure downstream (pandas'
	# "Expected object or value", no line/position given) can be traced
	# back to this stage specifically, with jq's own precise error location,
	# instead of guessing between this step and the p-value filter step in
	# run_finemap_pipeline.sh that consumes this file next.
	line_count = subprocess.run(['wc', '-l', 'input/input.json'], capture_output=True, text=True).stdout.strip()
	print(f"input/input.json: {line_count}")
	validate = subprocess.run(['jq', 'empty', 'input/input.json'], capture_output=True, text=True)
	if validate.returncode != 0:
		size = os.path.getsize('input/input.json')
		with open('input/input.json', 'rb') as f:
			head = f.read(500)
			f.seek(max(0, size - 500))
			tail = f.read(500)
		raise RuntimeError(
			f"input/input.json is not valid NDJSON. jq reports:\n{validate.stderr}\n"
			f"size: {size} bytes\nfirst 500 bytes: {head!r}\nlast 500 bytes: {tail!r}"
		)

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

	pheno_path = f'{s3_in}/out/metaanalysis/bottom-line/ancestry-specific/{args.phenotype}/ancestry={args.ancestry}/'
	var2rs_path = '/mnt/var/cojo/snps.csv'
	bfiles = '/mnt/var/cojo/bfiles'
	finemap_dir = '/mnt/var/cojo/finemapping'
	config_file = f'{finemap_dir}/analysis.config.yaml'
	out_path = f'{s3_out}/out/cojo/staging/{args.phenotype}/ancestry={args.ancestry}'

	# read all files in the clump path
	make_json_files(pheno_path)

	# create the tmp out directory
	out_directory = 'data'
	if not os.path.exists(out_directory):
		os.makedirs(out_directory, exist_ok=True)

	out_directory_full_path = os.path.abspath(out_directory)
	input_full_path = os.path.abspath('input')

	# check=True: the previous plain subprocess.call() discarded this return
	# code, so a real pipeline failure (e.g. run_finemap_pipeline.sh's own
	# "Error processing input data. Exiting.") still fell through to
	# touch _SUCCESS and upload below - the EMR step reported exitCode 0
	# even though fine-mapping never ran.
	subprocess.run(['bash', f'{finemap_dir}/run_finemap_pipeline.sh',
					'--input',input_full_path,
					'--bfiles', bfiles,
					'--config_file',config_file,
					'--dbsnp_file',var2rs_path,
					'--output', out_directory_full_path,
					'--finemap_dir',finemap_dir
					], check=True)

	subprocess.check_call(['touch', f'{out_directory}/_SUCCESS'])
	subprocess.check_call(['aws', 's3', 'cp', f'{out_directory}/', out_path, '--recursive'])
	safe_remove('input/input.json')
	shutil.rmtree('input')
	shutil.rmtree(out_directory)

if __name__ == '__main__':
	main()
