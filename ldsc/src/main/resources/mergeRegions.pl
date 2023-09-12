#!/usr/bin/perl
use strict;
use warnings;

my $s3dir="s3://dig-analysis-data/out/ldsc";

# which partition is being made
my $partition_name=$ARGV[0];
my $partition_in=$ARGV[1];
my $partition_out=$ARGV[2];

# create a temporary file to download all the partition data into
my $tmpFile="partitions.csv";
my $sortedFile="sorted.csv";
my $bedFile="$partition_name.csv";

# mergepart files together and sort it by position
`hadoop fs -getmerge -nl -skip-empty-file "$s3dir/regions/$partition_in" "$tmpFile"`;
`sort -k1,1 -k2,2n "$tmpFile" > "$sortedFile"`;

# open the sorted file and write to the bed file
open(IN, '<', $sortedFile) or die "Cannot read $sortedFile";
open(OUT, '>', $bedFile) or die "Cannot write $bedFile";

my $chr='';
my $start=0;
my $end=0;
my $state='';
my $biosample='';
my $method='';
my $source='';
my $dataset='';
# read each line
while (chomp(my $line=<IN>)) {
    if (not length $line) {
        next;
    }

    # split to get chrom, start, end
    my @pos=split("\t", $line);

    # merge with - and maybe expand - region if overlapping
    if (($pos[0] eq $chr) and ($pos[1] <= $end)) {
        if ($pos[2] > $end) {
            $end = int($pos[2]);
        }
    } else {
        if (length $chr) {
            print OUT "$chr\t$start\t$end\t$state\t$biosample\t$method\t$source\t$dataset\n";
        }
        $chr=$pos[0];
        $start=int($pos[1]);
        $end=int($pos[2]);
	    $state=$pos[3];
	    $biosample=$pos[4];
	    $method=$pos[5];
	    $source=$pos[6];
	    $dataset=$pos[7];
    }
}

if (length $chr) {
    print OUT "$chr\t$start\t$end\t$state\t$biosample\t$method\t$source\t$dataset\n";
}

# done
close IN;
close OUT;

# copy the final output file back to S3
`aws s3 cp "$bedFile" "${s3dir}/regions/$partition_out/$bedFile"`;

# delete the files to make room for other merges
unlink "$tmpFile";
unlink "$sortedFile";
unlink "$bedFile";
