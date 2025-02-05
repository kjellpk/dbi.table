sql_statement <- function(s) {
  sql_dir <- file.path(system.file(package = "dbi.table"), "sql_statements")
  f <- file.path(sql_dir, paste(s, "sql", sep = "."))
  DBI::SQL(readChar(f, file.info(f)$size))
}
