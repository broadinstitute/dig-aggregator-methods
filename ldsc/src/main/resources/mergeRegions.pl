#!/usr/bin/perl
use strict;
use warnings;

my $s3dir="s3://dig-analysis-data/out/ldsc";

# which partition is being made
my $partition=$ARGV[0];
my $sub_region=$ARGV[1];
# Have to figure out the sub region here to fix things

# create a temporary file to download all the partition data into
my $tmpFile="partitions.csv";
my $sortedFile="sorted.csv";
my $bedFile="$partition.csv";

# mergepart files together and sort it by position
`hadoop fs -getmerge -nl -skip-empty-file "$s3dir/regions/$sub_region/partitioned/*/partition=$partition/part-*" "$tmpFile"`;
`sort -u -k1,1 -k2,2n "$tmpFile" > "$sortedFile"`;

# open the sorted file and write to the bed file
open(IN, '<', $sortedFile) or die "Cannot read $sortedFile";
open(OUT, '>', $bedFile) or die "Cannot write $bedFile";

my $chr='';
my $start=0;
my $end=0;

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
            print OUT "$chr\t$start\t$end\n";
        }
        $chr=$pos[0];
        $start=int($pos[1]);
        $end=int($pos[2]);
    }
}

if (length $chr) {
    print OUT "$chr\t$start\t$end\n";
}

# done
close IN;
close OUT;

# copy the final output file back to S3
`aws s3 cp "$bedFile" "${s3dir}/regions/$sub_region/merged/$partition/$bedFile"`;

# delete the files to make room for other merges
unlink "$tmpFile";
unlink "$sortedFile";
unlink "$bedFile";
