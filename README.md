# `dbi.table`

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
    # install.packages("pak")
    pak::pak("kjellpk/dbi.table")

## Quick Start

First, let’s load the package.

    library(dbi.table)
    library(data.table) #for as.data.table

Next, create a zero-argument function that returns a `DBI` connection to
the Chinook database at the [CTU Prague Relational Learning
Repository](https://relational-data.org/).

    ctu_connector <- function() {
      DBI::dbConnect(RMariaDB::MariaDB(),
                     host = "relational.fel.cvut.cz",
                     port = 3306,
                     dbname = "Chinook",
                     user = "guest",
                     password = "ctu-relational")
    }

Using a function that creates a `DBI` connection rather than the `DBI`
connection itself (which also works) allows the `dbi.table` package to
manage the connection. The connection will be reestablished if it drops
and is disconnected when it is no longer needed.

    dbi.attach(ctu_connector)

The Chinook database is now attached to the search path in position 2.

    head(search(), 3)

    ## [1] ".GlobalEnv"         "RMariaDB:Chinook"   "package:data.table"

Its tables can be queried using `data.table`’s `[i, j, by]` syntax.

    Track[MediaTypeId == 1, .("#_of_Tracks" = .N), by = .(Composer)]

    ## <Chinook> Track 
    ##                                      Composer #_of_Tracks
    ##                                        <char>       <i64>
    ##                                            NA         629
    ##  A. F. Iommi, W. Ward, T. Butler, J. Osbourne           3
    ##                                      A. Jamal           1
    ##              A.Bouchard/J.Bouchard/S.Pearlman           1
    ##                    A.Isbell/A.Jones/O.Redding           1
    ##  ---

The `csql` utility displays the `dbi.table`’s SQL query.

    csql(Track[MediaTypeId == 1, .("#_of_Tracks" = .N), by = .(Composer)])

    ## SELECT `Track`.`Composer` AS `Composer`,
    ##        COUNT(*) AS `#_of_Tracks`
    ## 
    ##   FROM `Chinook`.`Track` AS `Track`
    ## 
    ##  WHERE `Track`.`MediaTypeId` = 1
    ## 
    ##  GROUP BY `Track`.`Composer`
    ## 
    ##  LIMIT 10000

Simply `detach` the schema when you are finished and the `DBI`
connection will be closed the next time garbage collection runs.

    detach("RMariaDB:Chinook")

## Supported Databases

As of version 1.0.4, `dbi.table` supports SQLite, DuckDB, Postgres
(RPostgres), and MariaDB (RMariaDB). Unsupported RDBMSs fallback to
DBI - not all features will be available. Full support can be added by
implementing a few S3 methods. Please feel free to open an issue if you
would like to use `dbi.table` with a particular RDBMS.
