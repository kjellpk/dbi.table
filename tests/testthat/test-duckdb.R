conn <- memory_connections[["memory_duckdb"]]

test_that("duckdb registered data.frames are unregistered on rm/gc", {
  x <- as.dbi.table(conn, datasets::iris, type = "temp")
  temp_table_name <- attr(x, "data_source", exact = TRUE)$id[[1]]@name

  tables <- DBI::dbGetQuery(conn, "SELECT name FROM (SHOW ALL TABLES)")$name
  expect_true(temp_table_name %in% tables)

  y <- x
  rm(x)
  gc()
  tables <- DBI::dbGetQuery(conn, "SELECT name FROM (SHOW ALL TABLES)")$name
  expect_true(temp_table_name %in% tables)

  rm(y)
  gc()
  tables <- DBI::dbGetQuery(conn, "SELECT name FROM (SHOW ALL TABLES)")$name
  expect_false(temp_table_name %in% tables)
})
