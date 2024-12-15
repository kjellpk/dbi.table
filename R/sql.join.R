#' Join \code{dbi.table}s
#'
#' A \code{SQL}-like join of two \code{\link{dbi.table}}s that share the
#' same \code{\link[DBI:DBIConnection-class]{DBI connection}}. All columns from
#' both \code{\link{dbi.table}}s are returned.
#'
#' @param x,y
#'   \code{\link{dbi.table}}s to join. \code{x} and \code{y} must share the same
#'   \code{\link[DBI:DBIConnection-class]{DBI connection}}.
#'
#' @param type
#'   a character string specifying the join type. Valid choices are
#'   \code{"inner"}, \code{"left"}, \code{"right"}, \code{"outer"}, and
#'   \code{"cross"}.
#'
#' @param on
#'   a \code{call} specifying the join predicate. The symbols in \code{on}
#'   should be column names of \code{x} or column names of \code{y}, use
#'   prefixes as necessary.
#'
#' @param prefixes
#'   a 2-element character vector of distinct values. When \code{x} and \code{y}
#'   both have a column with the same name (e.g., \code{common_name}) then, when
#'   specifing the join predicate in \code{on}, use
#'   \code{`prefixes[1]`common_name} to refer to the \code{common_name} column
#'   in \code{x} and \code{`prefixes[2]`common_name} to refer to the
#'   \code{common_name} column in \code{y}. \code{prefixes} are also used to
#'   disambiguate the output column names.
#'
#' @return
#'   a \code{dbi.table}.
#'
#' @examples
#' chinook <- dbi.catalog(chinook.duckdb)
#' Album <- chinook$main$Album
#' Artist <- chinook$main$Artist
#'
#' sql.join(Album, Artist, type = "inner",
#'          on = Album.ArtistId == Artist.ArtistId,
#'          prefixes = c("Album.", "Artist."))
#'
#' @export
sql.join <- function(x, y, type = "inner", on = NULL,
                     prefixes = c("x.", "y.")) {
  parent_frame <- parent.frame()

  if (!is.dbi.table(x)) {
    stop("'x' is not a 'dbi.table'")
  }

  if (!is.dbi.table(y)) {
    stop("'y' is not a 'dbi.table'")
  }

  if (!can_join_as_x(x)) {
    x <- as_cte(x)
  }

  x_ctes <- get_ctes(x)

  if (!can_join_as_y(y)) {
    y <- as_cte(y)
  }

  y_ctes <- get_ctes(y)

  type <- match.arg(type, choices = names(JOIN_TYPES))

  on_sub <- substitute(on)
  if (inherits(on <- try(on, silent = TRUE), "try-error")) {
    on <- on_sub
  }

  if (!is.null(on) && type == "cross") {
    stop("'on' must be 'NULL' when 'type' is \"cross\"")
  }

  if (is.null(on) && type != "cross") {
    stop("'on' cannot not be 'NULL' when 'type' is '", type, "'")
  }

  if (!is.null(on) && !is.call(on)) {
    stop("'on' is not a 'call'")
  }

  xref <- xref_in(x_names <- names(x), y_names <- names(y), prefixes)
  names(x) <- paste0(prefixes[[1L]], x_names)
  names(y) <- paste0(prefixes[[2L]], y_names)

  on_vars <- all.vars(on)

  if (any(i <- !(on_vars %chin% names(xref)))) {
    stop("ambiguous use of '", v <- on_vars[i][1], "' in 'on'; use '",
         paste0(prefixes[[1L]], v), "' to refer to the '", v, "' in 'x' ",
         "and '", paste0(prefixes[[2L]], v), "' to refer to the '", v,
         "' in 'y'")
  }


  # 1. Join column definitions

  on <- sub_lang(on, envir = xref, specials = NULL, enclos = parent_frame)

  x_fields <- get_fields(x)
  y_fields <- get_fields(y)
  y_sub <- paste0(session$key_base, nrow(x_fields) + seq_len(nrow(y_fields)))
  names(y_sub) <- y_fields$internal_name
  y_fields$internal_name <- unname(y_sub)
  y_sub <- lapply(y_sub, as.name)

  xy <- c(c(x), sub_lang(y, envir = y_sub))

  x_where <- get_where(x)
  if (length(y_where <- get_where(y))) {
    y_where <- sub_lang(y_where, envir = y_sub)
  }
  xy_where <- c(x_where, y_where)

  x_order_by <- get_order_by(x)
  if (length(y_order_by <- get_order_by(y))) {
    y_order_by <- sub_lang(y_order_by, envir = y_sub, specials = NULL)
  }
  xy_order_by <- c(x_order_by, y_order_by)

  # 2. Join DBI connections (fail if not same connection)

  if (!identical(xy_conn <- get_connection(x), get_connection(y))) {
    stop("'x' and 'y' do not share the same 'DBI' connection")
  }

  x_data_source <- get_data_source(x)
  y_data_source <- get_data_source(y)

  if (y_data_source$id_name %in% x_data_source$id_name) {
    y_data_source$id_name <- unique_table_name()
    y_fields$id_name <- y_data_source$id_name
  }


  # 3. join fields

  xy_fields <- rbind(x_fields, y_fields)


  # 5. join data_source

  y_data_source$clause <- JOIN_TYPES[type]

  if (!is.null(on)) {
    y_data_source$on <- list(sub_lang(on, envir = xy, specials = NULL))
  }

  xy_data_source <- rbind(x_data_source, y_data_source)


  # 6. join ctes

  xy_ctes <- c(x_ctes, y_ctes)
  xy_ctes <- xy_ctes[!duplicated(names(xy_ctes))]


  # 7. Set output names

  dups <- intersect(x_names, y_names)
  idx <- x_names %chin% dups
  x_names[idx] <- paste0(prefixes[[1L]], x_names[idx])
  idx <- y_names %chin% dups
  y_names[idx] <- paste0(prefixes[[2L]], y_names[idx])

  names(xy) <- c(x_names, y_names)
  dbi_table_object(cdefs = xy,
                   conn = xy_conn,
                   data_source = xy_data_source,
                   fields = xy_fields,
                   where = xy_where,
                   order_by = xy_order_by,
                   ctes = xy_ctes)
}



JOIN_TYPES <- c(inner = "INNER JOIN",
                left  = "LEFT OUTER JOIN",
                right = "RIGHT OUTER JOIN",
                outer = "FULL OUTER JOIN",
                cross = "CROSS JOIN")



can_join_as_x <- function(x) {
  dbi_table_is_simple(x)
}



can_join_as_y <- function(x) {
  data_source <- get_data_source(x)
  dbi_table_is_simple(x) && (nrow(data_source) == 1L)
}



xref_in <- function(x_names, y_names, prefixes) {
  prefixes <- as.character(prefixes)
  if ((length(prefixes) != 2) || anyDuplicated(prefixes)) {
    stop("'prefixes' is not a character vector containing 2 distinct values",
         call. = FALSE)
  }

  if (nchar(prefixes[[2L]]) < 1L) {
    stop("'prefixes[2]' must have at least 1 character", call. = FALSE)
  }

  dups <- intersect(x_names, y_names)

  input_names <- c(x_names, y_names)
  prefixed_names <- c(paste0(prefixes[[1L]], x_names),
                      paste0(prefixes[[2L]], y_names))

  if (idx <- anyDuplicated(prefixed_names)) {
    stop("prefixed names are not unique - first duplicate: ",
         prefixed_names[idx], call. = FALSE)
  }

  xref <- prefixed_names
  names(prefixed_names) <- prefixed_names

  names(xref) <- input_names
  keep <- setdiff(setdiff(input_names, dups), prefixed_names)
  xref <- xref[keep]

  xref <- c(prefixed_names, xref)
  names_list(xref, names(xref))
}
