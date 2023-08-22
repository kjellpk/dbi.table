`dbi.table` generates and executes SQL queries from `data.table` syntax.

## Why `dbi.table`?

-   use `data.table` syntax to fetch `data.table`s from a database over
    a `DBI` connection
-   attach databases to the search path using `dbi.attach`

## Installation

    devtools::install_github("kjellpk/dbi.table")

## Usage

First, load the package.

    library(dbi.table)

Start with an arbitrary `DBI` connection (the function `ex_chinook`
returns a connection to the Chinook sample data that is included in the
`dbi.table` package).

    (conn <- ex_chinook())

    ## Loading required namespace: RSQLite

    ## <SQLiteConnection>
    ##   Path: /Library/Frameworks/R.framework/Versions/4.2-arm64/Resources/library/dbi.table/example_files/Chinook_Sqlite.sqlite
    ##   Extensions: TRUE

As you can see, bog-standard `DBI` connection - in this case facilitated
by `RSQLite`.

Create a `dbi.table` corresponding to the `Album` table in the Chinook
database.

    library(DBI) # For Id
    (Album <- dbi.table(conn, Id(table = "Album")))

    ## <Chinook_Sqlite> Album 
    ##  AlbumId                                 Title ArtistId
    ##        1 For Those About To Rock We Salute You        1
    ##        2                     Balls to the Wall        2
    ##        3                     Restless and Wild        2
    ##        4                     Let There Be Rock        1
    ##        5                              Big Ones        3
    ## ---
