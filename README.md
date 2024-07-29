# `dbi.table`

Query database objects (e.g., tables and views) accessible over a `DBI`
connection using `data.table`’s `[i, j, by]` syntax. Under the hood, a
`dbi.table` is a just an SQL query - its methods are designed to give it
`data.table` look and feel.

## Installation

    devtools::install_github("kjellpk/dbi.table")

## Quick Start

First, load the package.

    library(dbi.table)

Next, open a DBI connection (big thank you to the [CTU Prague Relational
Learning Repository](https://relational-data.org/) for providing this
example).

    ctu_conn <- DBI::dbConnect(RMariaDB::MariaDB(),
                               host = "relational.fel.cvut.cz",
                               port = 3306,
                               user = "guest",
                               password = "ctu-relational")

Then make a `dbi.table` based on the `Album` table in the `Chinook`
schema.

    (Album <- dbi.table(ctu_conn, DBI::Id("Chinook", "Album")))

    ## <relational.fel.cvut.cz> Album 
    ##  AlbumId                                 Title ArtistId
    ##    <int>                                <char>    <int>
    ##        1 For Those About To Rock We Salute You        1
    ##        2                     Balls to the Wall        2
    ##        3                     Restless and Wild        2
    ##        4                     Let There Be Rock        1
    ##        5                              Big Ones        3
    ##  ---

A `dbi.table` looks like a `data.table` (but with the row numbers
omitted). Under the hood, a `dbi.table` is a SQL query - the printed
output here is a 5 row preview. This query can be maniulated using
`data.table`’s `[i, j, by]` syntax.

    (x <- Album[nchar(Title) > 20 & AlbumId > ArtistId,
                .(Title, Title_Length = paste(nchar(Title), "characters"))])

    ## <relational.fel.cvut.cz> Album 
    ##                                     Title  Title_Length
    ##                                    <char>        <char>
    ##            Plays Metallica By Four Cellos 30 characters
    ##                  The Best Of Billy Cobham 24 characters
    ##  Alcohol Fueled Brewtality Live! [Disc 1] 40 characters
    ##  Alcohol Fueled Brewtality Live! [Disc 2] 40 characters
    ##           Black Sabbath Vol. 4 (Remaster) 31 characters
    ##  ---

Some data wrangling can be done on-database. The `csql` utility displays
the underlying SQL query (SQL generation uses `dbplyr::translate_sql_`).

    csql(x)

    ## 
    ## SELECT `Album`.`Title` AS `Title`,
    ##        CONCAT_WS(' ', LENGTH(`Album`.`Title`), 'characters') AS `Title_Length`
    ## 
    ##   FROM `Chinook`.`Album` AS `Album`
    ## 
    ##  WHERE LENGTH(`Album`.`Title`) > 20 AND `Album`.`AlbumId` > `Album`.`ArtistId`
    ## 
    ##  LIMIT 10000

Finally, close the DBI connection.

    DBI::dbDisconnect(ctu_conn)
