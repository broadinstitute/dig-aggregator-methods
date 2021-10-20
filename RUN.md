# Method Running

The [README][readme] file explains _how_ to run the methods via the command line. This document is about the methods themselves and the order to run the methods as several are dependent on each other.

For the details of any given method, please see the README file in the method sub-folder.

## Brief Order

If just with no command-line arguments, this is the general order the methods should be run:

1. vep
2. bottom-line
4. gregor
5. magma
6. finemapping
7. basset
8. burden-binning
9. bioindex (*)

_(*) The `bioindex` method should __always__ be run last as it takes the output of all the other methods and prepares it for indexing!_

## Method Dependencies

To learn what the actual dependencies are for any given method, refer to the `sources` variable of the `Stage` classes in the method sub-folders.

This is a brief outline of which methods require input from other methods:

```
VEP         -> Fine Mapping, Basset, MAGMA, Burden Binning
Bottom Line -> GREGOR, MAGMA, Fine Mapping
*           -> BioIndex
```

As can be seen, `vep` and `bottom-line` are essentially the two core methods on which all other methods depend (and they do not depend on each other). So, as long as `vep` and `bottom-line` are run first, then the others can be run in any order (*).

## Deprecated Methods

These methods are deprecated and shouldn't be run. They are still in the repo for posterity or in case they ever need to be resurrected.

* frequencyanalysis - replaced by 1000g and gnomad AF data in dbNSFP output by vep


[readme]: README.md
