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
  type <- match.arg(type, choices = names(JOIN_TYPES))

  if (!is.dbi.table(x)) {
    stop("'x' is not a 'dbi.table'")
  }

  if (!is.dbi.table(y)) {
    stop("'y' is not a 'dbi.table'")
  }

  on_sub <- substitute(on)
  if (inherits(on <- stry(on), "try-error")) {
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

  on_vars <- all.vars(on)
  if (any(i <- (on_vars %in% intersect(names(x), names(y))))) {
    stop("ambiguous use of '", v <- on_vars[i][1], "' in 'on'; use '",
         paste0(prefixes[[1L]], v), "' to refer to the '", v, "' in 'x' ",
         "and '", paste0(prefixes[[2L]], v), "' to refer to the '", v,
         "' in 'y'")
  }

  prefixes <- as.character(prefixes)
  if ((length(prefixes) != 2) || anyDuplicated(prefixes)) {
    stop("'prefixes' is not a character vector containing 2 distinct values")
  }

  if (nchar(prefixes[[2L]]) < 1L) {
    stop("'prefixes[2]' must have at least 1 character")
  }

  if (!can_join_as_x(x)) {
    x <- as_cte(x)
  }

  if (!can_join_as_y(y)) {
    y <- as_cte(y)
  }

  sql_join(x, y, type, on, prefixes, parent.frame())
}


sql_join <- function(x, y, type, on, prefixes, parent_frame) {

  x_ctes <- get_ctes(x)
  y_ctes <- get_ctes(y)


  # 1. Join fields and column definitions

  x_fields <- get_fields(x)
  y_fields <- get_fields(y)

  old_internal_name <- y_fields$internal_name
  new_name <- paste0(KEY_BASE, nrow(x_fields) + seq_len(nrow(y_fields)))  
  y_fields$internal_name <- new_name
  y_sub <- names_list(y_fields$internal_name, old_internal_name)
  new_y <- sub_lang(y, y_sub)

  names_x <- names(x)
  names_y <- names(y)
  dups <- intersect(names_x, names_y)

  pre_x <- c(x)
  names(pre_x) <- paste0(prefixes[[1L]], names(pre_x))

  uni_x <- c(x)[names_x %notin% names_y]

  pre_y <- new_y
  names(pre_y) <- paste0(prefixes[[2L]], names(pre_y))

  uni_y <- new_y[names(new_y) %notin% names_x]

  xref <- c(pre_x, uni_x, pre_y, uni_y)

  xy <- c(c(x), new_y)
  on <- sub_lang(on, xref, enclos = parent_frame)

  # join where

  y_where <- sub_lang(get_where(y), y_sub)
  xy_where <- c(get_where(x), y_where)

  # join order_by

  y_order_by <- sub_lang(get_order_by(y), y_sub)
  xy_order_by <- c(get_order_by(x), y_order_by)

  # 2. Join DBI connections (fail if not same connection)

  if (!identical(xy_conn <- get_connection(x), get_connection(y))) {
    stop("'x' and 'y' do not share the same 'DBI' connection")
  }

  x_data_source <- get_data_source(x)
  y_data_source <- get_data_source(y)

  for (nm in y_data_source$id_name) {
    if (nm %in% x_data_source$id_name) {
      new_nm <- unique_table_name()
      y_data_source[[y_data_source$id_name == nm, "id_name"]] <- new_nm
      y_fields$id_name <- gsub(nm, new_nm, y_fields$id_name, fixed = TRUE)
    }
  }

  xy_fields <- rbind_f(x_fields, y_fields)


  # 5. join data_source

  y_data_source$on <- lapply(y_data_source$on, sub_lang, envir = y_sub)

  y_data_source[[1L, "clause"]] <- JOIN_TYPES[type]
  y_data_source$on[1L] <- list(on)[1L]

  xy_data_source <- rbind(x_data_source, y_data_source)


  # 6. join ctes

  xy_ctes <- c(x_ctes, y_ctes)
  xy_ctes <- xy_ctes[!duplicated(names(xy_ctes))]


  # 7. Set output names

  dups <- intersect(x_names <- names(x), y_names <- names(y))
  idx <- x_names %in% dups
  x_names[idx] <- paste0(prefixes[[1L]], x_names[idx])
  idx <- y_names %in% dups
  y_names[idx] <- paste0(prefixes[[2L]], y_names[idx])

  names(xy) <- c(x_names, y_names)
  dbi_table_object(cdefs = xy, conn = xy_conn, data_source = xy_data_source,
                   fields = xy_fields, where = xy_where, order_by = xy_order_by,
                   ctes = xy_ctes)
}



JOIN_TYPES <- c(inner = "INNER JOIN",
                left  = "LEFT OUTER JOIN",
                right = "RIGHT OUTER JOIN",
                outer = "FULL OUTER JOIN",
                cross = "CROSS JOIN")



can_join_as_x <- function(x) {
  !has_over(x) &&
    !length(get_where(x)) &&
    !length(get_group_by(x)) &&
    !get_distinct(x)
}



can_join_as_y <- function(x) {
  !has_over(x) &&
    !length(get_group_by(x)) &&
    !get_distinct(x) &&
    (nrow(get_data_source(x)) == 1L)
}



paste_prefix <- function(p, n) {
  paste0(rep(p, length(n)), n)
}
