conn <- memory_connections[["memory_sqlite"]]

test_that("SQLite temporary tables are dropped on rm/gc", {
  x <- as.dbi.table(conn, datasets::iris, type = "temp")
  temp_table_name <- attr(x, "data_source", exact = TRUE)$id[[1]]@name

  tables <- DBI::dbGetQuery(conn, "SELECT name FROM sqlite_temp_master")$name
  expect_true(temp_table_name %in% tables)

  y <- x
  rm(x)
  gc()
  tables <- DBI::dbGetQuery(conn, "SELECT name FROM sqlite_temp_master")$name
  expect_true(temp_table_name %in% tables)

  rm(y)
  gc()
  tables <- DBI::dbGetQuery(conn, "SELECT name FROM sqlite_temp_master")$name
  expect_false(temp_table_name %in% tables)
})
