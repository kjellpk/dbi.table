JOINS <- c(inner = "INNER JOIN",
           left  = "LEFT OUTER JOIN",
           right = "RIGHT OUTER JOIN",
           outer = "FULL OUTER JOIN",
           cross = "CROSS JOIN")


#' Join dbi.tables
#'
#' @description Perform a \code{SQL}-like join on two \code{\link{dbi.table}}s
#'              that share the same underlying \code{\link{DBI}} connection.
#'              All columns of both \code{\link{dbi.table}}s are returned.
#'
#' @param x,y \code{\link{dbi.table}}s to join. \code{x} and \code{y} must
#'            reference objects on the same \code{\link{DBI}} connection.
#'
#' @param type one of \dQuote{inner}, \dQuote{left}, \dQuote{right},
#'             \dQuote{outer}, or \dQuote{cross}.
#'
#' @param on a \code{\link{call}} to translate to the \code{SQL} \code{ON}
#'           clause.
#'
#' @param env an environment, \code{on} is partially evaluated in \code{env}
#'            when this function is evaluated (i.e., not when the join is
#'            translated to \code{SQL}.)
#'
#' @param prefixes a character vector of 2 elements. When \code{x} and \code{y}
#'                 both have a column with the same name, prefixes are used to
#'                 eliminate the ambiguity.
#'
#' @param suffixes a character vector of 2 elements. When \code{x} and \code{y}
#'                 both have a column with the same name, suffixes are appended
#'                 to eliminate the ambiguity.
#'
#' @export
join <- function(x, y, type = "inner", on = on(), env = parent.frame(),
                 prefixes = c("x.", "y."), suffixes = c(".x", ".y")) {

  xy_sub <- c(x_sub <- deparse(substitute(x)), y_sub <- deparse(substitute(y)))

  if (!is.dbi.table(x)) {
    stop(sQuote("x"), " is not a dbi.table")
  }

  if (!is.dbi.table(y)) {
    stop(sQuote("y"), " is not a dbi.table")
  }

  if (!dbi.table_can_join_x(x)) {
    x <- as_cte(x)
  }

  x_ctes <- get_ctes(x)

  if (!dbi.table_is_simple(y)) {
    y <- as_cte(y)
  }

  y_ctes <- get_ctes(y)

  type <- match.arg(type, choices = names(JOINS))

  if (inherits(try(on, silent = TRUE), "try-error")) {
    if (as.character((on <- substitute(on))[[1]]) == "on") {
      on <- as.list(on)[-1]

      if (any(idx <- !vapply(on, is.call, FALSE))) {
        stop(sQuote(format(on[[which(idx)[1]]])), " is not a call")
      }

      on <- handy_andy(on)
    }
  }

  if (!is.call(on)) {
    stop(sQuote("on"), " is not a call")
  }

  if ((length(prefixes <- as.character(prefixes)) != 2) ||
      any(duplicated(prefixes))) {
    stop(sQuote("prefixes"), " is not a character vector ",
         "containing 2 distinct values")
  }

  if (is.null(on)) {
    if (type != "cross") {
      stop(sQuote("on"), " cannot not be ", sQuote("NULL"),
           " when ", sQuote("type"), " is ", sQuote(type))
    }
  } else {
    if (!is.call(on)) {
      stop(sQuote("on"), " is not a call")
    } else {
      if (!any(duplicated(xy_sub)) && all(xy_sub == make.names(xy_sub))) {
        tmp_prefix <- lapply(paste0(prefixes[1], names(x)), as.name)
        names(tmp_prefix) <- paste(xy_sub[1], names(x), sep = ".")
        on <- sub_lang(on, cdefs = tmp_prefix)

        tmp_prefix <- lapply(paste0(prefixes[2], names(y)), as.name)
        names(tmp_prefix) <- paste(xy_sub[2], names(y), sep = ".")
        on <- sub_lang(on, cdefs = tmp_prefix)
      }

      tmp_prefix <- lapply(paste0(prefixes[1], names(x)), as.name)
      names(tmp_prefix) <- names(x)
      on <- sub_lang(on, cdefs = tmp_prefix)

      tmp_prefix <- lapply(paste0(prefixes[2], names(y)), as.name)
      names(tmp_prefix) <- names(y)
      on <- sub_lang(on, cdefs = tmp_prefix)
    }
  }

  on_vars <- get_names(on)
  dups <- intersect(names(x), names(y))

  if (any(idx <- (on_vars %in% dups))) {
    stop("ambiguous use of ", sQuote(v <- on_vars[idx][1]), " in ",
         sQuote("on"), "; use ", sQuote(paste0(prefixes[1], v)),
         " to refer to the ", sQuote(v), " in ", sQuote(x_sub), " and ",
         sQuote(paste0(prefixes[2], v)), " to refer to the ", sQuote(v),
         " in ", sQuote(y_sub))
  }

  suffixed_names_x <- names(x)
  idx <- (suffixed_names_x %in% dups)
  suffixed_names_x[idx] <- paste0(suffixed_names_x[idx], suffixes[1])
  suffixed_names_y <- names(y)
  idx <- (suffixed_names_y %in% dups)
  suffixed_names_y[idx] <- paste0(suffixed_names_y[idx], suffixes[2])
  suffixed_names <- c(suffixed_names_x, suffixed_names_y)

  names(x) <- paste0(prefixes[1], names(x))
  names(y) <- paste0(prefixes[2], names(y))

  if (get_hash(x) == get_hash(y)) {
    conn <- get_connection(x)
  } else {
    stop(sQuote("x"), " and ", sQuote("y"), " do not share the same ",
         sQuote("DBI"), " connection")
  }

  x_data_source <- get_data_source(x)
  x_fields <- get_fields(x)

  y_data_source <- get_data_source(y)
  y_fields <- get_fields(y)

  if (nrow(y_data_source) > 1) {
    stop("case not handled yet") #shouldn't happen ever
  }

  if (y_data_source$id_name %in% x_data_source$id_name) {
    yid <- unique_table_name()

    if (yid %chin% x_data_source$id_name) {
      stop("self join failed") #Could this ever happen??? Really???
    }

    y_data_source$id_name <- yid
    y_fields$id_name <- yid
  }


  # 3. join fields

  nx <- nrow(x_fields)
  ny <- nrow(y_fields)

  new_name <- paste0("FN", (nx + 1):(nx + ny))
  names(new_name) <- y_fields$internal_name
  y_fields$internal_name <- new_name

  y <- lapply(c(y), sub_lang, cdefs = lapply(new_name, as.name))

  fields <- rbind(x_fields, y_fields)


  # 4. Join columns

  xy <- c(x, y)

  # 5. join data_source

  y_data_source$clause <- JOINS[type]

  if (!is.null(on)) {
    y_data_source$on <- list(prepare_call(on, xy, env))
  }

  data_source <- rbind(x_data_source, y_data_source)


  # 6. join ctes

  ctes <- c(x_ctes, y_ctes)
  ctes <- ctes[!duplicated(names(ctes))]

  names(xy) <- suffixed_names

  dbi_table_object(xy, hash_connection(conn), data_source, fields,
                   ctes = ctes)
}



#' Join ON
#'
#' @describeIn join
#'
#' @description A helper to make the \dQuote{on} argument.
#'
#' @param \dots arguements for the \dQuote{on} clause.
#'
#' @section Value a single call.
on <- function(...) {
  NULL
}
