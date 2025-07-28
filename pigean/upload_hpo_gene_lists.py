#!/usr/bin/env python3

import pandas as pd
import boto3
import argparse
import sys
import os
import hashlib
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

"""
HPO file available from https://hpo.jax.org/data/annotations (choose phenotype to genes)
"""

def read_hpo_file(filepath):
    """Read HPO data file with tab-separated values."""
    try:
        df = pd.read_csv(filepath, sep='\t')
        return df
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        sys.exit(1)

def filter_phenotypes_with_multiple_genes(df):
    """Filter to phenotypes that have 2-1000 genes."""
    gene_counts = df.groupby('hpo_id')['gene_symbol'].nunique()
    valid_phenotypes = gene_counts[(gene_counts >= 2) & (gene_counts <= 1000)].index
    filtered_df = df[df['hpo_id'].isin(valid_phenotypes)]
    
    total_phenotypes = df['hpo_id'].nunique()
    too_few = len(gene_counts[gene_counts < 2])
    too_many = len(gene_counts[gene_counts > 1000])
    
    print(f"Found {len(valid_phenotypes)} phenotypes with 2-1000 genes")
    print(f"Excluded: {too_few} with <2 genes, {too_many} with >1000 genes")
    print(f"Total phenotypes processed: {total_phenotypes}")
    
    return filtered_df

def create_gene_list_content(genes, probability=0.95):
    """Create TSV content with genes and probability."""
    content = []
    for gene in genes:
        content.append(f"{gene}\t{probability}")
    return '\n'.join(content)

def upload_to_s3(s3_client, bucket, key, content):
    """Upload content to S3, skip if content hash matches existing ETag."""
    try:
        # Calculate MD5 hash of our content
        content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
        
        # Check if object exists and get its ETag
        try:
            response = s3_client.head_object(Bucket=bucket, Key=key)
            existing_etag = response['ETag'].strip('"')  # Remove quotes from ETag
            
            if existing_etag == content_hash:
                return {'success': True, 'key': key, 'error': None, 'skipped': True}
        except Exception as e:
            # Object doesn't exist or other error - proceed with upload
            if hasattr(e, 'response') and e.response.get('Error', {}).get('Code') != 'NoSuchKey':
                # Log unexpected errors but continue
                pass
        
        # Upload the content
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content
        )
        return {'success': True, 'key': key, 'error': None, 'skipped': False}
    except Exception as e:
        return {'success': False, 'key': key, 'error': str(e), 'skipped': False}

def sanitize_hpo_id(hpo_id):
    """Replace colons in HPO ID for S3 key compatibility."""
    return hpo_id.replace(':', '_')

def main():
    parser = argparse.ArgumentParser(description='Process HPO data and upload gene lists to S3')
    parser.add_argument('input_file', help='Input TSV file with HPO data')
    parser.add_argument('--probability', type=float, default=0.95, help='Default probability for genes (default: 0.95)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be uploaded without actually uploading')
    parser.add_argument('--max-workers', type=int, default=20, help='Maximum concurrent uploads (default: 20)')
    parser.add_argument('--bucket', default='dig-analysis-data', help='S3 bucket name (default: dig-analysis-data)')
    
    args = parser.parse_args()
    
    print("Reading and processing data...")
    df = read_hpo_file(args.input_file)
    filtered_df = filter_phenotypes_with_multiple_genes(df)
    
    bucket = args.bucket
    
    # Pre-compute all upload tasks
    print("Preparing upload tasks...")
    upload_tasks = []
    for hpo_id in filtered_df['hpo_id'].unique():
        phenotype_genes = filtered_df[filtered_df['hpo_id'] == hpo_id]['gene_symbol'].unique()
        content = create_gene_list_content(phenotype_genes, args.probability)
        sanitized_id = sanitize_hpo_id(hpo_id)
        s3_key = f"out/pigean/inputs/gene_lists/hpo/{sanitized_id}/gene_list.tsv"
        
        upload_tasks.append({
            'hpo_id': hpo_id,
            'sanitized_id': sanitized_id,
            'key': s3_key,
            'content': content,
            'gene_count': len(phenotype_genes)
        })
        
        # Add _SUCCESS file for this directory
        success_key = f"out/pigean/inputs/gene_lists/hpo/{sanitized_id}/_SUCCESS"
        upload_tasks.append({
            'hpo_id': hpo_id,
            'sanitized_id': sanitized_id,
            'key': success_key,
            'content': '',
            'gene_count': 0
        })
    
    print(f"Prepared {len(upload_tasks)} upload tasks")
    
    if args.dry_run:
        print("Dry run - showing sample tasks:")
        for i, task in enumerate(upload_tasks[:5]):
            print(f"  Would upload {task['gene_count']} genes for {task['hpo_id']} to s3://{bucket}/{task['key']}")
        if len(upload_tasks) > 5:
            print(f"  ... and {len(upload_tasks) - 5} more")
        print(f"Dry run complete. Would upload {len(upload_tasks)} gene lists")
        return
    
    # Initialize S3 client and perform concurrent uploads
    s3_client = boto3.client('s3')
    uploaded_count = 0
    skipped_count = 0
    failed_count = 0
    
    print(f"Starting concurrent uploads with {args.max_workers} workers...")
    
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        # Submit all upload tasks
        future_to_task = {
            executor.submit(upload_to_s3, s3_client, bucket, task['key'], task['content']): task
            for task in upload_tasks
        }
        
        # Process results as they complete
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            result = future.result()
            
            if result['success']:
                if result.get('skipped', False):
                    skipped_count += 1
                else:
                    uploaded_count += 1
                
                processed_count = uploaded_count + skipped_count
                if processed_count % 100 == 0:
                    print(f"Processed {processed_count}/{len(upload_tasks)} files (uploaded: {uploaded_count}, skipped: {skipped_count})...")
            else:
                failed_count += 1
                print(f"Failed to upload {task['hpo_id']}: {result['error']}")
    
    print(f"Upload complete. Uploaded: {uploaded_count}, Skipped: {skipped_count}, Failed: {failed_count}")

if __name__ == "__main__":
    main()
