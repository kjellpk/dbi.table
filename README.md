# `dbi.table`

Query database objects (e.g., tables and views) accessible over a `DBI`
connection using `data.table`-like syntax. Access individual objects
using `dbi.table` or attach an entire schema using `dbi.attach`.

## Installation

    devtools::install_github("kjellpk/dbi.table")

## Usage

First, load the package.

    library(dbi.table)

Then open up a `DBIConnection` to the [CTU Prague Relational Learning
Repository](https://relational-data.org/).

    rdp_conn <- DBI::dbConnect(RMariaDB::MariaDB(),
                               host = "relational.fel.cvut.cz",
                               port = 3306,
                               user = "guest",
                               password = "ctu-relational")

Make an `Id` for the `Album` table in the `Chinook` schema.

    album_table_id <- DBI::Id(table_schema = "Chinook", table_name = "Album")

Finally, create the `dbi.table`.

    (Album <- dbi.table(rdp_conn, album_table_id))

    ## <relational.fel.cvut.cz> Album 
    ##  AlbumId                                 Title ArtistId
    ##    <int>                                <char>    <int>
    ##        1 For Those About To Rock We Salute You        1
    ##        2                     Balls to the Wall        2
    ##        3                     Restless and Wild        2
    ##        4                     Let There Be Rock        1
    ##        5                              Big Ones        3
    ##  ---

The `print` method fetches the first 5 records from the database and
displays them as a `data.table` (with the row numbers omitted). The
underlying SQL query can be viewed using the `csql` function.

    csql(Album)

    ## 
    ## SELECT `Album`.`AlbumId` AS `AlbumId`,
    ##        `Album`.`Title` AS `Title`,
    ##        `Album`.`ArtistId` AS `ArtistId`
    ## 
    ##   FROM `Chinook`.`Album` AS `Album`
    ## 
    ##  LIMIT 10000

The `dbi.table` can be manipulated using `data.table` syntax.

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

Again, the underlying SQL query can be viewed with `csql`.

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
