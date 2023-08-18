JOINS <- c(inner = "INNER JOIN",
           left  = "LEFT OUTER JOIN",
           right = "RIGHT OUTER JOIN",
           outer = "FULL OUTER JOIN",
           cross = "CROSS JOIN")


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
#' @param env an environment, \code{on} is partially evaluated in \code{env}
#'            when this function is evaluated (i.e., not when the join is
#'            translated to \code{SQL}.)
#'
#' @param prefixes a 2-element character vector. When \code{x} and \code{y}
#'                 both have a column with the same name, prefixes are used to
#'                 eliminate the ambiguity.
#'
#' @export
join <- function(x, y, type = "inner", on = NULL, env = parent.frame(),
                 prefixes = c("x.", "y.")) {
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

  on_sub <- substitute(on)

  if (inherits(on <- try(on, silent = TRUE), "try-error")) {
    if (on_sub[[1]] == as.name(".") || on_sub[[1]] == as.name("list")) {
      if (length(on <- as.list(on_sub)[-1]) > 1) {
        on <- handy_andy(on)
      } else {
        on <- on[[1]]
      }
    }
  } else {
    if (is.list(on) && all(vapply(on, is.call, FALSE))) {
      on <- handy_andy(on)
    }
  }

  stopifnot(is.null(on) || is.call(on))

  if ((length(prefixes <- as.character(prefixes)) != 2) ||
      any(duplicated(prefixes))) {
    stop(sQuote("prefixes"), " is not a character vector ",
         "containing 2 distinct values")
  }

  # Define to avoid spurious R CMD check note
  dup <- name <- out <- pname <- NULL

  d <- rbind(data.table(source = "x",
                        name = names(x),
                        pname = paste0(prefixes[1], names(x))),
              data.table(source = "y",
                        name = names(y),
                        pname = paste0(prefixes[2], names(y))))
  d[, dup := name %chin% intersect(names(x), names(y))]
  d[, out := ifelse(dup, pname, name)]

  if (is.null(on)) {
    if (type != "cross") {
      stop(sQuote("on"), " cannot not be ", sQuote("NULL"),
           " when ", sQuote("type"), " is ", sQuote(type))
    }
  } else {
    if (!is.call(on)) {
      stop(sQuote("on"), " is not a call")
    }

    #partial eval on here

    l <- lapply(d[dup == FALSE][["pname"]], as.name)
    names(l) <- d[dup == FALSE][["name"]]
    on <- sub_lang(on, l)

    on_vars <- get_names(on)

    if (any(idx <- (on_vars %in% d[dup == TRUE, unique(name)]))) {
      stop("ambiguous use of ", sQuote(v <- on_vars[idx][1]), " in ",
          sQuote("on"), "; use ", sQuote(paste0(prefixes[1], v)),
          " to refer to the ", sQuote(v), " in ", sQuote("x"), " and ",
          sQuote(paste0(prefixes[2], v)), " to refer to the ", sQuote(v),
          " in ", sQuote("y"))
    }
  }

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

  if (y_data_source$id_name %in% x_data_source$id_name) {
    y_data_source$id_name <- yid <- unique_table_name()
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

  names(xy) <- d$out

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
#' @section Value a list of \code{\link[base]{call}}s.
#' @export
on <- function(...) {
  m <- as.list(match.call())[-1]

  if (any(not_a_call <- !vapply(m, is.call, FALSE))) {
    stop(sQuote(format(m[not_a_call][[1]])), " is not a call")
  }

  m
}
