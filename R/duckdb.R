
catalog_dbi_table_templates_duckdb <- function(catalog, ...) {
  w <- if (is.null(schema <- list(...)$schema)) {
    ";"
  } else {
    paste0("\n WHERE schema_name = ", as.character(schema)[[1L]], ";")
  }

  statement <- paste0(sql_statement("duckdb_columns"), w)
  templates <- DBI::dbGetQuery(catalog, statement)

  f <- apply(templates[, c("catalog", "schema", "table")],
             1L, paste, collapse = ".")
  templates <- split(templates, f)

  templates <- lapply(templates, function(u, catalog) {
    env <- new.env(parent = topenv())
    assign("catalog", catalog, pos = env)
    assign("id", DBI::Id(unlist(u[1L, c("catalog", "schema", "table")])),
           pos = env)

    fields <- u[u$column_index, c("column", "column_type")]
    names(fields) <- c("field", "type")
    assign("fields", fields, pos = env)

    assign("primary_key", NULL, pos = env)
    assign("foreign_keys", NULL, pos = env)

    env
  }, catalog = catalog)

  w <- if (is.null(schema)) {
    ";"
  } else {
    paste0("\n   AND schema_name = ", as.character(schema)[[1L]], ";")
  }

  statement <- paste0(sql_statement("duckdb_primary_keys"), w)
  pk <- DBI::dbGetQuery(catalog, statement)
  pk$f <- apply(pk[, 1:3], 1L, paste, collapse = ".")

  lapply(split(pk, pk$f), function(u, objects) {
    objects[[u[[1, "f"]]]]$primary_key <- u$column[u$column_index]
  }, objects = objects)

  objects
}




#' @rawNamespace S3method(default_schema,duckdb_connection,default_schema_duckdb)
default_schema_duckdb <- function(conn) {
  DBI::dbGetQuery(conn, "SELECT current_schema()")[[1L]]
}



#' @rawNamespace S3method(schemas_to_include,duckdb_connection,schemas_to_include_duckdb)
schemas_to_include_duckdb <- function(conn) {
  c("information_schema")
}



#' @rawNamespace S3method(foreign_keys_,duckdb_connection,foreign_keys_duckdb)
foreign_keys_duckdb <- function(catalog, id) {
  r <- merge(catalog$information_schema$referential_constraints,
             catalog$information_schema$key_column_usage,
             by = c("constraint_catalog",
                    "constraint_schema",
                    "constraint_name"))

  names(r) <- paste0("fk_", names(r))

  r <- merge(catalog$information_schema$key_column_usage, r,
             by.x = c("constraint_catalog",
                      "constraint_schema",
                      "constraint_name",
                      "ordinal_position"),
             by.y = c("fk_unique_constraint_catalog",
                      "fk_unique_constraint_schema",
                      "fk_unique_constraint_name",
                      "fk_ordinal_position"))

  r <- r[, c("fk_constraint_name", "fk_table_catalog", "fk_table_schema",
              "fk_table_name", "fk_column_name", "table_catalog",
              "table_schema", "table_name", "column_name",
              "fk_position_in_unique_constraint")]

  names(r) <- c("constraint_name", "fk_table_catalog", "fk_table_schema",
                "fk_table_name", "fk_column_name", "pk_table_catalog",
                "pk_table_schema", "pk_table_name", "pk_column_name",
                "key_ordinal_position")

  as.data.frame(r, n = -1L)
}



#' @rawNamespace S3method(temporary_dbi_table,duckdb_connection,temporary_dbi_table_duckdb)
temporary_dbi_table_duckdb <- function(conn, x, key = NULL) {
  temp_name <- unique_table_name(TMP_BASE)
  duckdb::duckdb_register(conn, temp_name, x)

  temp_id <- DBI::Id(temp_name)
  x <- new_dbi_table(conn, temp_id, names(x), key)

  temp_dbi_table <- new.env(parent = emptyenv())
  temp_dbi_table$name <- temp_name
  temp_dbi_table$conn <- conn
  reg.finalizer(temp_dbi_table, finalize_duckdb_temp_dbi_table)

  attr(x, "temp_dbi_table") <- temp_dbi_table
  x
}



finalize_duckdb_temp_dbi_table <- function(e) {
  stry(duckdb::duckdb_unregister(e$conn, e$name))
  NULL
}
