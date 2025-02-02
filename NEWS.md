**If you are viewing this file on CRAN, please check the [latest news on GitHub](https://github.com/kjellpk/dbi.table/blob/master/NEWS.md) where the formatting is also better.**


# dbi.table v1.0.3

## NOTES

1. `dbi.table`s in schemas are now implemented as active bindings so that `data.table`'s `setcolorder` will not change their column order.

2. In `reference.test`, when the output of `all.equal` is not `TRUE` and `verbose = TRUE`, the format of the `message` now matches `print(all.equal(...))`. Thanks to @MichaelChirico for the report and fix.


# dbi.table v1.0.1 (10 December 2024)

1. Initial CRAN release.
