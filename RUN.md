# Method Running

The [README][readme] file explains _how_ to run the methods via the command line. This document is about the methods themselves and the order to run the methods as several are dependent on each other.

For the details of any given method, please see the README file in the method sub-folder.

## Brief Order

If just with no command-line arguments, this is the general order the methods should be run:

1. vep
2. bottom-line
3. ldsc
4. gregor
5. magma
6. burden-binning
7. bioindex (*)

_(*) The `bioindex` method should __always__ be run last as it takes the output of all the other methods and prepares it for indexing!_

## Method Dependencies

To learn what the actual dependencies are for any given method, refer to the `sources` variable of the `Stage` classes in the method sub-folders.

This is a brief outline of which methods require input from other methods.  Here, `A -> B` means "A is a prerequisite of B":

```
VEP         -> MAGMA, Burden Binning
Bottom Line -> LDSC, GREGOR, MAGMA
LDSC        -> GREGOR
*           -> BioIndex
```

As can be seen, `vep` and `bottom-line` are essentially the two core methods on which all other methods depend (and they do not depend on each other). The `gregor` method also uses a few outputs of a couple `ldsc` stages. So, as long as `vep`, `bottom-line`, and `ldsc` are run first, then the others can be run in any order (*).

A suggested order of processor execution, that was used for the last couple of runs:

```
VEP, Bottom Line, GREGOR, MAGMA, BioIndex
```

Note that it's convenient to run the BioIndex Processor Stages individually, interleaved with rebuilding affected indexes, to minimize downtime of the dev Bioindex instance.  A script to do this is here:

https://github.com/broadinstitute/dig-bioindex/tree/master/automation


[readme]: README.md
