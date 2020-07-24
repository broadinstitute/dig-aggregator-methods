#!/usr/bin/perl

use strict;
use warnings;

# runVEP.sh script location
my $s3_script = 's3://dig-analysis-data/resources/Vep/runVEP.sh';

# copy the runVEP.sh script from S3 to this location
if (system("aws s3 cp $s3_script .") != 0) {
    die "Failed to copy $s3_script from S3";
}

# spawn the shell script for each part
foreach my $part (@ARGV) {
    my $pid = fork();

    die "fork: $!" unless defined($pid);

    if (not $pid) {
        exec("bash ./runVEP.sh $part");
    }
}

# wait for all processes to finish
while ((my $pid = wait()) >= 0) {
    die "$pid failed with status: $?" if $? != 0;
}
