
<!-- README.md is generated from README.Rmd. Please edit that file -->

# `dbi.table`

Query and manipulate database objects (e.g., tables, views) accessible
over a `DBI` connection using `data.table`-like syntax.

## Why `dbi.table`?

- concise syntax: fast to type, fast to read - in fact, even easier than
  `data.table` because there aren’t as many features
- as fast as the underlying database
- memory efficient - R operations modify queries, not data
- no API lifecycle management
- just me
- includes features

## Test knitr on github

``` r
if (require(dbi.table)) {
  print("dbi.table available")
} else {
  print("dbi.table notavailable")
}
```

    ## Loading required package: dbi.table

    ## Warning in library(package, lib.loc = lib.loc, character.only = TRUE,
    ## logical.return = TRUE, : there is no package called 'dbi.table'

    ## [1] "dbi.table notavailable"
