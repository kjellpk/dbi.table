dbi_table_template <- function(id, catalog, fields) {
  self <- new.env(parent = topenv())
  assign(".self", self, pos = self)

  assign(".catalog", catalog, pos = self)
  catalog_fun <- function(x) {
    if (!missing(x)) stop("binding is read-only")
    .self$.catalog
  }
  environment(catalog_fun) <- self
  makeActiveBinding("catalog", catalog_fun, self)

  assign(".id", id, pos = self)
  id_fun <- function(x) {
    if (!missing(x)) stop("binding is read-only")
    .self$.id
  }
  environment(id_fun) <- self
  makeActiveBinding("id", id_fun, self)

  if (is.data.frame(fields)) {
    assign(".fields", fields, self)
  } else {
    assign(".fields_fun", fields, self)
  }

  fields_fun <- function(x) {
    if (! missing(x)) stop("binding is read-only")

    if (is.null(.self$.fields)) {
      fields <- .self$.fields_fun(.self$catalog, .self$id)
      assign(".fields", fields, .self)
    }

    .self$.fields
  }
  environment(fields_fun) <- self
  makeActiveBinding("fields", fields_fun, self)

  pk_fun <- function(x) {
    if (!missing(x)) stop("binding is read-only")
    .self$.primary_key
  }
  environment(pk_fun) <- self
  makeActiveBinding("primary_key", pk_fun, self)

  fk_fun <- function(x) {
    if (!missing(x)) stop("binding is read-only")
    .self$.foreign_keys
  }
  environment(fk_fun) <- self
  makeActiveBinding("foreign_keys", fk_fun, self)

  constructor_fun <- function(x) {
    if (!missing(x)) stop("binding is read-only")

    new_dbi_table(conn = .self$catalog,
                  id = .self$id,
                  fields = copy_data_frame(.self$fields),
                  primary_key = .self$primary_key,
                  foreign_keys = .self$foreign_keys)
  }
  environment(constructor_fun) <- self
  assign("constructor", constructor_fun, self)

  self
}


id_name <- function(id, conn) {
  DBI::dbQuoteIdentifier(conn, id)
}
