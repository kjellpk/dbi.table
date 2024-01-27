#' Join dbi.tables
#'
#' @description Perform a \code{SQL}-like join on two \code{\link{dbi.table}}s
#'              that share the same underlying \code{\link[DBI]{DBI}}
#'              connection. All columns of both \code{\link{dbi.table}}s are
#'              returned.
#'
#' @param x,y \code{\link{dbi.table}}s to join. \code{x} and \code{y} must
#'            reference objects on the same \code{\link[DBI]{DBI}} connection.
#'
#' @param type one of \dQuote{inner}, \dQuote{left}, \dQuote{right},
#'             \dQuote{outer}, or \dQuote{cross}.
#'
#' @param on a \code{\link[base]{call}} to translate to the \code{SQL} \code{ON}
#'           clause.
#'
#' @param envir an environment, \code{on} is partially evaluated in \code{envir}
#'              when this function is evaluated (i.e., not when the join is
#'              translated to \code{SQL}.)
#'
#' @param prefixes a 2-element character vector. When \code{x} and \code{y}
#'                 both have a column with the same name, prefixes are used to
#'                 eliminate the ambiguity.
#'
#' @export
join <- function(x, y, type = "inner", on = NULL, envir = parent.frame(),
                 prefixes = c("x.", "y.")) {
  if (!is.dbi.table(x)) {
    stop(sQuote("x"), " is not a dbi.table")
  }

  if (!is.dbi.table(y)) {
    stop(sQuote("y"), " is not a dbi.table")
  }

  if (!can_join_as_x(x)) {
    x <- as_cte(x)
  }

  x_ctes <- get_ctes(x)

  if (!can_join_as_y(y)) {
    y <- as_cte(y)
  }

  y_ctes <- get_ctes(y)

  type <- match.arg(type, choices = names(JOINS))

  on_sub <- substitute(on)

  if (inherits(on <- try(on, silent = TRUE), "try-error")) {
    on <- on_sub
  }

  if (is.null(on) && type != "cross") {
    stop(sQuote("on"), " cannot not be ", sQuote("NULL"), " when ",
         sQuote("type"), " is ", sQuote(type))
  }

  if (!is.null(on) && !is.call(on)) {
    stop(sQuote("on"), " is not a ", sQuote("call"))
  }

  prefixes <- as.character(prefixes)
  if ((length(prefixes) != 2) || anyDuplicated(prefixes)) {
    stop(sQuote("prefixes"), " is not a character vector ",
         "containing 2 distinct values")
  }

  names_df <- rbind(data.table(input = "x",
                               input_name = names(x),
                               prefixed_name = paste0(prefixes[1], names(x))),
                    data.table(input = "y",
                               input_name = names(y),
                               prefixed_name = paste0(prefixes[2], names(y))))

  dups <- names_df[, input_name[duplicated(input_name)]]
  names_df[, duplicated := (input_name %chin% dups)]

  # For R CMD check
  output_name <- prefixed_name <- input_name <- NULL

  names_df[, output_name := ifelse(duplicated, prefixed_name, input_name)]

  if (any(i <- ((on_vars <- all.vars(on)) %chin% dups))) {
    stop("ambiguous use of ", sQuote(v <- on_vars[i][1]), " in ",
         sQuote("on"), "; use ", sQuote(paste0(prefixes[1], v)),
         " to refer to the ", sQuote(v), " in ", sQuote("x"), " and ",
         sQuote(paste0(prefixes[2], v)), " to refer to the ", sQuote(v),
         " in ", sQuote("y"))
  }


  # 1. Join column definitions

  ip <- names_df[duplicated == FALSE, prefixed_name]
  names(ip) <- names_df[duplicated == FALSE, input_name]
  pp <- names_df[, prefixed_name]
  names(pp) <- names_df[, prefixed_name]
  lookup <- sapply(c(ip, pp), as.name, simplify = FALSE)

  on <- sub_lang(on, dbi_table = lookup, specials = NULL, env = envir)

  x_fields <- get_fields(x)
  y_fields <- get_fields(y)
  y_sub <- paste0(session$key_base, nrow(x_fields) + seq_len(nrow(y_fields)))
  names(y_sub) <- y_fields$internal_name
  y_fields$internal_name <- y_sub
  y_sub <- lapply(y_sub, as.name)

  xy <- c(c(x), lapply(c(y), sub_lang, dbi_table = y_sub, specials = NULL))
  names(xy) <- names_df[, prefixed_name]


  # 2. Join DBI connections (fail if not same connection)

  if (identical(attr(x, "conn", exact = TRUE), attr(y, "conn", exact = TRUE))) {
    conn <- attr(x, "conn", exact = TRUE)
  } else {
    stop(sQuote("x"), " and ", sQuote("y"), " do not share the same ",
         sQuote("DBI"), " connection")
  }

  x_data_source <- get_data_source(x)
  y_data_source <- get_data_source(y)

  if (y_data_source$id_name %in% x_data_source$id_name) {
    y_data_source$id_name <- unique_table_name()
    y_fields$id_name <- y_data_source$id_name
  }

  # 3. join fields

  fields <- rbind(x_fields, y_fields)

  # 5. join data_source

  y_data_source$clause <- JOINS[type]

  if (!is.null(on)) {
    y_data_source$on <- list(sub_lang(on, dbi_table = xy, specials = NULL))
  }

  data_source <- rbind(x_data_source, y_data_source)

  # 6. join ctes

  ctes <- c(x_ctes, y_ctes)
  ctes <- ctes[!duplicated(names(ctes))]


  # 7. Set output names
  names(xy) <- names_df[, output_name]

  dbi_table_object(xy, conn, data_source, fields, ctes = ctes)
}



on <- function(...) {
  m <- as.list(match.call())[-1]

  if (any(not_a_call <- !vapply(m, is.call, FALSE))) {
    stop(sQuote(format(m[not_a_call][[1]])), " is not a call")
  }

  handy_andy(m)
}



JOINS <- c(inner = "INNER JOIN",
           left  = "LEFT OUTER JOIN",
           right = "RIGHT OUTER JOIN",
           outer = "FULL OUTER JOIN",
           cross = "CROSS JOIN")



can_join_as_x <- function(x) {
  dbi_table_is_simple(x)
}



can_join_as_y <- function(x) {
  data_source <- attr(x, "data_source", exact = TRUE)
  dbi_table_is_simple(x) && (nrow(data_source) == 1L)
}
