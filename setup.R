library(DBI)
con <- dbConnect(RSQLite::SQLite(), ":memory:")

for(dfn in ls("package:datasets")) {
  df <- get(dfn, envir = as.environment("package:datasets"))
  if(inherits(df, "data.frame"))
    dbWriteTable(con, dfn, sqlRownamesToColumn(as.data.frame(df)))
}



library(DBI.table)
attachDBI(conn = con)

