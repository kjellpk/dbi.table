# `dbi.table`

Query database objects (e.g., tables and views) accessible over a `DBI`
connection using `data.table`-like syntax. Access individual objects
using `dbi.table` or attach an entire schema using `dbi.attach`.

## Installation

    devtools::install_github("kjellpk/dbi.table")

## Usage

First, load the package.

    library(dbi.table)

Open a `DBI` connection to the Chinook database included in the package.

    db_path <- file.path(system.file(package = "dbi.table"),
                         "example_files",
                         "chinook_sqlite.sqlite")
    chinook <- DBI::dbConnect(RSQLite::SQLite(), db_path)

Create a `dbi.table` using the `Album` table in the Chinook database.

    (Album <- dbi.table(chinook, DBI::Id(table = "Album")))

    ## <chinook_sqlite> Album 
    ##  AlbumId                                 Title ArtistId
    ##    <int>                                <char>    <int>
    ##        1 For Those About To Rock We Salute You        1
    ##        2                     Balls to the Wall        2
    ##        3                     Restless and Wild        2
    ##        4                     Let There Be Rock        1
    ##        5                              Big Ones        3
    ## ---

The local object `Album` is a `dbi.table`, a data structure that
generates and executes queries on the remote (SQLite) table `Album`.
`dbi.table`s use the same syntax as `data.tables`. For example, the
following command selects the `AlbumId` and `Title` fields from the
remote table `Album` where `AlbumId` is less than 100 and equal to
`ArtistId`.

    (x <- Album[AlbumId < 100 & AlbumId == ArtistId, .(AlbumId, Title)])

    ## <chinook_sqlite> Album 
    ##  AlbumId                                 Title
    ##    <int>                                <char>
    ##        1 For Those About To Rock We Salute You
    ##        2                     Balls to the Wall
    ##       58                   Come Taste The Band

By default, the preview is the first 5 rows returned by the dbms (in
this case SQLite). There are only 3 records that satify the query so all
results were returned.

The local object `x` is not the data, rather it is another `dbi.table`
corresponding to the specific query. Use empty square brackets (`[]`) or
the `as.data.table` function to fetch the results set as a `data.table`.

    x[]

    ##    AlbumId                                 Title
    ##      <int>                                <char>
    ## 1:       1 For Those About To Rock We Salute You
    ## 2:       2                     Balls to the Wall
    ## 3:      58                   Come Taste The Band
