#!/usr/bin/env python3

import pandas as pd
import boto3
import argparse
import sys

def read_hpo_file(filepath):
    """Read HPO data file with tab-separated values."""
    try:
        df = pd.read_csv(filepath, sep='\t')
        return df
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        sys.exit(1)

def generate_code_to_name(df, output_file=None):
    """Generate code_to_name.tsv with unique hpo_id and hpo_name pairs."""
    unique_hpo = df[['hpo_id', 'hpo_name']].drop_duplicates().sort_values('hpo_id')
    
    # Replace colons with underscores in HPO IDs
    unique_hpo['hpo_id'] = unique_hpo['hpo_id'].str.replace(':', '_')
    
    if output_file:
        unique_hpo.to_csv(output_file, sep='\t', index=False, header=False)
        print(f"Generated {output_file} with {len(unique_hpo)} unique HPO terms")
    
    return unique_hpo

def upload_to_s3(content, bucket, key):
    """Upload content to S3."""
    try:
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content
        )
        print(f"Uploaded to s3://{bucket}/{key}")
        return True
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Generate code_to_name.tsv from HPO data')
    parser.add_argument('input_file', help='Input TSV file with HPO data')
    parser.add_argument('-o', '--output', help='Output TSV file (default: code_to_name.tsv if not uploading)')
    parser.add_argument('--upload', action='store_true', help='Upload to S3 instead of saving locally')
    parser.add_argument('--bucket', default='dig-analysis-bin', help='S3 bucket for upload (default: dig-analysis-bin)')
    parser.add_argument('--s3-key', default='hpo/code_to_name.tsv', help='S3 key for upload (default: hpo/code_to_name.tsv)')
    
    args = parser.parse_args()
    
    df = read_hpo_file(args.input_file)
    
    if args.upload:
        # Generate content and upload to S3
        unique_hpo = generate_code_to_name(df)
        content = unique_hpo.to_csv(sep='\t', index=False, header=False)
        
        if upload_to_s3(content, args.bucket, args.s3_key):
            print(f"Successfully uploaded {len(unique_hpo)} unique HPO terms to S3")
        else:
            sys.exit(1)
    else:
        # Save to local file
        output_file = args.output or 'code_to_name.tsv'
        generate_code_to_name(df, output_file)

if __name__ == "__main__":
    main()