#' @export
print.DBI.table <- function(x, topn = 5,  ...)
{
  query <- dbplyr::select_query(from = dbplyr::sql(x$name),
                                select = dbplyr::sql(names(x)),
                                limit = topn)
  query.s <- dbplyr::sql_render(query, con = x$envir$.channel)
  #print(query.s)

  d <- data.table(DBI::dbGetQuery(x$envir$.channel, query.s, stringsAsFactors = FALSE))
  print(d, nrow = topn)
  cat("  ...\n")
  invisible(x)
}


#' @export
"[.DBI.table" <- function (x, i)
{
  if(missing(i))
    i <- character()
  else {
    i <- dbplyr::partial_eval(substitute(i), vars = names(x))
    i <- dbplyr::translate_sql_(list(i))
  }

  query <- dbplyr::select_query(from = dbplyr::sql(x$name),
                                select = dbplyr::sql(names(x)),
                                where = i)
  query.s <- dbplyr::sql_render(query, con = x$envir$.channel)

  #print(query.s)
  #cat("\n")

  data.table(DBI::dbGetQuery(x$envir$.channel, query.s, stringsAsFactors = FALSE))[]
}


#' @export
names.DBI.table <- function(x)
{
  if(is.null(x$fields))
    x$envir[[x$name]]$fields <- dbListFields(x$envir$.channel, x$name)

  x$envir[[x$name]]$fields
}


#' @export
length.DBI.table <- function(x)
  base::length(names(x))


#' @export
dimnames.DBI.table <- function(x)
  list(NULL, names(x))


#' @export
dim.DBI.table <- function(x)
  c(NA_integer_, base::length(names(x)))


