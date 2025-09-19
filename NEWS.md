**If you are viewing this file on CRAN, please check the [latest news on GitHub](https://github.com/kjellpk/dbi.table/blob/master/NEWS.md) where the formatting is also better.**


# dbi.table v1.0.5

## NOTES

1. Bug fix. `as_cte` was putting the CTEs in the wrong order when the input
   already included a CTE. This caused DuckDB >= 1.4.0 to bork.


# dbi.table v1.0.4

## NOTES

1. RDBMS-specific functions are now generic and existing code has been moved to methods. Methods are provided for RMariaDB, RPostgres, RSQLite, duckdb, and Microsoft SQL Server. The default methods rely on DBI alone and should provide basic functionality where DBI is fully implemented.

2. `data.table`-style keys are partially implemented. A key can be specified when creating a `dbi.table`. `dbi.table`s created by `dbi.catalog` (and `dbi.attach`) use the table's primary key as the default.

3. The column naming scheme for relational merge (i.e., `merge(x)`) has been changed.


# dbi.table v1.0.3

## NOTES

1. `dbi.table`s in schemas are now implemented as active bindings so that `data.table`'s `setcolorder` does not change their column order.

2. In `reference.test`, when the output of `all.equal` is not `TRUE` and `verbose = TRUE`, the format of the `message` now matches `print(all.equal(...))`. Thanks to @MichaelChirico for the report and fix.


# dbi.table v1.0.1 (10 December 2024)

1. Initial CRAN release.
