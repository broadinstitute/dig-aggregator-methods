# Method Running

The [README][readme] file explains _how_ to run the methods via the command line. This document is about the methods themselves and the order to run the methods as several are dependent on each other.

For the details of any given method, please see the README file in the method sub-folder.

## Brief Order

If just with no command-line arguments, this is the general order the methods should be run:

1. intake
2. gene-associations
3. vep
4. bottom-line
5. ldsc
6. magma
7. huge
8. burden-binning
9. basset
10. bioindex (*)

_(*) The `bioindex` method should __always__ be run last as it takes the output of all the other methods and prepares it for indexing!_

## Method Dependencies

To learn what the actual dependencies are for any given method, refer to the `sources` variable of the `Stage` classes in the method sub-folders.

This is a brief outline of which methods require input from other methods.  Here, `A -> B` means "A is a prerequisite of B":

```
Intake      -> VEP, Bottom Line
VEP         -> MAGMA, Burden Binning, Basset
Bottom Line -> LDSC, MAGMA
MAGMA       -> Huge
*           -> BioIndex
```


A suggested order of processor execution, that was used for the last couple of runs:

```
Intake, Gene Associations, VEP, Bottom Line, LDSC, MAGMA, Burden Binning, Basset, Huge, BioIndex
```

Note that it's convenient to run the BioIndex Processor Stages individually, interleaved with rebuilding affected indexes, to minimize downtime of the dev Bioindex instance.  A script to do this is here:

https://github.com/broadinstitute/dig-bioindex/tree/master/automation


[readme]: README.md
