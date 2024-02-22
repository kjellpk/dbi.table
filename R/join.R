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
#' @param type a character string. Valid choices are \code{"inner"},
#'             \code{"left"}, \code{"right"}, \code{"outer"},
#'             and \code{"cross"}.
#'
#' @param on a \code{\link[base]{call}} to translate to the \code{SQL} \code{ON}
#'           clause.
#'
#' @param envir an environment, \code{on} is partially evaluated in \code{envir}
#'              when this funcion is evaluated (i.e., not when the join is
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

  if (!is.null(on) && type == "cross") {
    warning("ignoring ", sQuote("on"), " argument for cross join")
    on <- NULL
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

  if (nchar(prefixes[[2L]]) < 1L) {
    stop("'prefixes[2]' must have at least 1 character")
  }

  x_names <- names(x)
  y_names <- names(y)

  prefixed_x_names <- paste0(prefixes[[1L]], x_names)
  prefixed_y_names <- paste0(prefixes[[2L]], y_names)

  dups <- intersect(x_names, y_names)
  allowed <- intersect(dups, prefixed_x_names)

  on_vars <- all.vars(on)

  if (any(i <- ((on_vars) %chin% setdiff(dups, allowed)))) {
    stop("ambiguous use of ", sQuote(v <- on_vars[i][1]), " in ",
         sQuote("on"), "; use ", sQuote(paste0(prefixes[1], v)),
         " to refer to the ", sQuote(v), " in ", sQuote("x"), " and ",
         sQuote(paste0(prefixes[2], v)), " to refer to the ", sQuote(v),
         " in ", sQuote("y"))
  }

  if (any(setdiff(y_names, allowed) %chin% prefixed_x_names) ||
        any(setdiff(x_names, allowed) %chin% prefixed_y_names)) {
    stop("this combination of column names and prefixes is not supported")
  }


  # 1. Join column definitions

  prefixed_names <- c(prefixed_x_names, prefixed_y_names)
  input_names <- names_list(prefixed_names, c(x_names, y_names))
  prefixed_names <- names_list(prefixed_names)
  input_names <- input_names[!(names(input_names) %chin% dups)]

  on <- sub_lang(on, envir = c(prefixed_names, input_names), specials = NULL)

  names(x) <- prefixed_x_names
  names(y) <- prefixed_y_names

  x_fields <- get_fields(x)
  y_fields <- get_fields(y)
  y_sub <- paste0(session$key_base, nrow(x_fields) + seq_len(nrow(y_fields)))
  names(y_sub) <- y_fields$internal_name
  y_fields$internal_name <- unname(y_sub)
  y_sub <- lapply(y_sub, as.name)

  xy <- c(c(x), sub_lang(y, envir = y_sub, specials = NULL))


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
    y_data_source$on <- list(sub_lang(on, envir = xy, specials = NULL))
  }

  data_source <- rbind(x_data_source, y_data_source)


  # 6. join ctes

  ctes <- c(x_ctes, y_ctes)
  ctes <- ctes[!duplicated(names(ctes))]


  # 7. Set output names

  output_names <- c(x_names, y_names)
  repl <- output_names %chin% dups
  output_names[repl] <- as.character(prefixed_names)[repl]
  names(xy) <- output_names


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
