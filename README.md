# `dbi.table`

<!-- badges: start -->

[![R-CMD-check](https://github.com/kjellpk/dbi.table/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/kjellpk/dbi.table/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

The `dbi.table` package allows you to query database tables and views
over a `DBI` connection using `data.table`’s `[i, j, by]` syntax. The
package provides functions for connecting to a single table, for
attaching a database schema to the search path, and for connecting to an
entire database catalog. When using schemas and catalogs, `dbi.table`
uses a table’s primary key as the default key when creating locally
instantiated `data.table`s and uses a table’s foreign key as the default
for `by` when merging.

## Installation

    # Install dbi.table from CRAN:
    install.packages("dbi.table")

    # Or the development version from GitHub:
    # install.packages("remotes")
    remotes::install_github("kjellpk/dbi.table")

## Quick Start

First, let’s load the package.

    library(dbi.table)
    library(data.table) #for as.data.table

While the `dbi.table` package will work with `DBIConnection` handles
(e.g., as returned by `DBI::dbConnect`), best practice is instead to use
a zero argument function that returns a `DBIConnection`. This paradigm
allows the package to open, close, and reconnect to the database as
necessary.

The function `chinook.duckbd` returns an open `DBIConnection` handle to
the sample *Chinook* database included in the package.

    dbi.attach(chinook.duckdb)

The Chinook database is now attached to the search path in position 2.

    head(search(), 3)

    ## [1] ".GlobalEnv"            "duckdb:chinook_duckdb" "package:data.table"

Its tables can be queried using `data.table`’s `[i, j, by]` syntax.

    Track[MediaTypeId == 1, .("#_of_Tracks" = .N), by = .(Composer)]

    ## <chinook_duckdb> Track 
    ##                                             Composer #_of_Tracks
    ##                                               <char>       <num>
    ##  Steven Tyler, Joe Perry, Jim Vallance, Holly Knight           1
    ##                              Al Perkins/Willie Dixon           1
    ##                             Fausto Nilo - Armandinho           1
    ##               Bino Farias/Da Gama/Lazão/Toni Garrido           4
    ##                              Marisa Monte/Nando Reis           2
    ##  ---

The `csql` utility displays the `dbi.table`’s SQL query.

    csql(Track[MediaTypeId == 1, .("#_of_Tracks" = .N), by = .(Composer)])

    ## SELECT Track.Composer AS Composer,
    ##        COUNT(*) AS "#_of_Tracks"
    ## 
    ##   FROM chinook_duckdb.main.Track AS Track
    ## 
    ##  WHERE Track.MediaTypeId = 1
    ## 
    ##  GROUP BY Track.Composer
    ## 
    ##  LIMIT 10000

Simply `detach` the schema when you are finished and the `DBI`
connection will be closed the next time garbage collection runs.

    detach("duckdb:chinook_duckdb")

## Supported Databases

As of version 1.0.4, `dbi.table` supports SQLite, DuckDB, Postgres
(RPostgres), and MariaDB (RMariaDB). Unsupported RDBMSs fallback to
DBI - not all features will be available. Full support can be added by
implementing a few S3 methods. Please feel free to open an issue if you
would like to use `dbi.table` with a particular RDBMS.
