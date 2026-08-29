### Load data.table ############################################################

library(data.table)
setDTthreads(1L)


### Set up open DBI connections to Chinook database ############################

chinook_connections <- list(
  chinook_sqlite = chinook.sqlite(),

  chinook_duckdb = {
    duck <- chinook.duckdb()
    DBI::dbExecute(duck, "SET threads TO 1;")
    duck
  },

  chinook_mssql = {
    conn <- DBI::dbConnect(
      odbc::odbc(),
      Driver   = "ODBC Driver 18 for SQL Server",
      Server   = "127.0.0.1,1433",
      UID      = "SA",
      PWD      = "YourStrong@Passw0rd",
      Encrypt  = "yes",
      TrustServerCertificate = "yes"
    )
    attr(conn, "schema") <- "dbo"

    DBI::dbExecute(conn, "USE [master];")
    DBI::dbExecute(conn,
     "IF EXISTS (SELECT 1 FROM sys.databases WHERE [name] = 'chinook')
        BEGIN ALTER DATABASE [chinook] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
      END")
    DBI::dbExecute(conn, "DROP DATABASE IF EXISTS [chinook];")
    DBI::dbExecute(conn, "CREATE DATABASE [chinook];")
    DBI::dbExecute(conn, "USE [chinook];")
    load_chinook_database(conn)
  }
)

## Set duckdb to use only 1 thread ##
#DBI::dbExecute(chinook_connections$chinook_duckdb, "SET threads TO 1;")


### Set up open DBI connections to :memory: database ###########################

memory_connections <- list(
  memory_sqlite = DBI::dbConnect(RSQLite::SQLite(), ":memory:"),

  memory_duckdb = {
    duck <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
    DBI::dbExecute(duck, "SET threads TO 1;")
    duck
  }
)

## Set duckdb to use only 1 thread ##
#DBI::dbExecute(memory_connections$memory_duckdb, "SET threads TO 1;")


### Teardown - close DBI connections ###########################################

withr::defer({
  lapply(chinook_connections, DBI::dbDisconnect)
  lapply(memory_connections, DBI::dbDisconnect)
}, teardown_env())
