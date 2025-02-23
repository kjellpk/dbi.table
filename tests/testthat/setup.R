### Load data.table ############################################################

library(data.table)
setDTthreads(1L)


### Set up open DBI connections to Chinook database ############################

chinook_connections <- list(
  chinook_sqlite = chinook.sqlite(),
  chinook_duckdb = chinook.duckdb()
)

## Set duckdb to use only 1 thread ##
DBI::dbExecute(chinook_connections$chinook_duckdb, "SET threads TO 1;")


### Set up open DBI connections to :memory: database ###########################

memory_connections <- list(
  memory_sqlite = DBI::dbConnect(RSQLite::SQLite(), ":memory:"),
  memory_duckdb = DBI::dbConnect(duckdb::duckdb(), ":memory:")
)

## Set duckdb to use only 1 thread ##
DBI::dbExecute(memory_connections$memory_duckdb, "SET threads TO 1;")


### Teardown - close DBI connections ###########################################

withr::defer({
  lapply(chinook_connections, DBI::dbDisconnect)
  lapply(memory_connections, DBI::dbDisconnect)
}, teardown_env())
