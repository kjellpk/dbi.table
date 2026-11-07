#' @rawNamespace S3method(catalog_objects,SQLiteConnection,catalog_objects_sqlite)
catalog_objects_sqlite <- function(catalog, ...) {
  schema <- DBI::dbGetQuery(catalog, sql_statement("sqlite_columns"))
  ids <- apply(schema, 1L, DBI::Id)
  names(ids) <- vapply(ids, id_name, "", conn = catalog)

  fields_fun <- function(conn, id) {
    r <- DBI::dbSendQuery(catalog, sql_statement("sqlite_fields"))
    on.exit(DBI::dbClearResult(r))
    DBI::dbBind(r, unname(as.list(id@name)[c("table", "schema")]))
    DBI::dbFetch(r)
  }

  list(ids = ids, fields_fun = fields_fun)
}



#' @rawNamespace S3method(catalog_primary_keys,SQLiteConnection,catalog_primary_keys_sqlite)
catalog_primary_keys_sqlite <- function(catalog, ...) {
  function(conn, id) {
    r <- DBI::dbSendQuery(catalog, sql_statement("sqlite_primary_keys"))
    on.exit(DBI::dbClearResult(r))
    DBI::dbBind(r, unname(as.list(id@name)[c("table", "schema")]))
    DBI::dbFetch(r)
  }
}



#' @rawNamespace S3method(catalog_foreign_keys,SQLiteConnection,catalog_foreign_keys_sqlite)
catalog_foreign_keys_sqlite <- function(catalog, ...) {
  function(conn, id) {
    r <- DBI::dbSendQuery(catalog, sql_statement("sqlite_foreign_keys"))
    on.exit(DBI::dbClearResult(r))
    DBI::dbBind(r, unname(as.list(id@name)[c("table", "schema")]))
    fks <- DBI::dbFetch(r)
    fks$ref_table_schema <- id@name[["schema"]]
    fk_df_to_list(fks, catalog)
  }
}
