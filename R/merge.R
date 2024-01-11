#' @export
merge.dbi.table <- function(x, y, by = NULL, by.x = NULL, by.y = NULL,
                            all = FALSE, all.x = all, all.y = all,
                            sort = TRUE, suffixes = c(".x", ".y"), ...) {
  if (!is.dbi.table(x)) {
    stop(sQuote("x"), " is not a ", sQuote("dbi.table"))
  }

  if (!is.dbi.table(y)) {
    stop(sQuote("y"), " is not a ", sQuote("dbi.table"))
  }

  if (anyDuplicated(nx <- names(x)) || anyDuplicated(ny <- names(y))) {
    stop("the ", sQuote("merge"), " method for ", sQuote("dbi.table"),
         " requires that ", sQuote("x"), " and ", sQuote("y"),
         " each have unique column names")
  }

  if ((!is.null(by.x) || !is.null(by.y)) && length(by.x) != length(by.y))
    stop(sQuote("by.x"), " and ", sQuote("by.y"), " are not the same length")

  if (!missing(by) && !missing(by.x))
      warning("specification of ", sQuote("by"), " superseded by ",
              sQuote("by.x"), " and ", sQuote("by.y"))

  if (!is.null(by.x)) {
    if (length(by.x) == 0L || !is.character(by.x) || !is.character(by.y))
      stop("A non-empty vector of column names is required for ",
           sQuote("by.x"), " and ", sQuote("by.y"))

    if (!all(by.x %chin% nx))
      stop("Elements listed in ", sQuote("by.x"),
           " must be valid column names in ",
           sQuote("x"))

    if (!all(by.y %chin% ny))
      stop("Elements listed in ", sQuote("by.y"),
           " must be valid column names in ",
           sQuote("y"))
  } else {
    if (!length(by)) {
      by <- intersect(nx, ny)
    }

    if (!all(by %chin% intersect(nx, ny)))
      stop("Elements listed in ", sQuote("by"),
           " must be valid column names in ",
           sQuote("x"), " and ", sQuote("y"))

    by <- unname(by)
    by.x <- by.y <- by
  }

  by.xx <- paste0("x.", by.x)
  by.yy <- paste0("y.", by.y)

  on <- lapply(paste(by.xx, by.yy, sep = " == "), str2lang)
  on <- handy_andy(on)

  if (!length(by.x)) {
    type <- "cross"
  } else if (all.x && all.y) {
    type <- "outer"
  } else if (all.x && !all.y) {
    type <- "left"
  } else if (!all.x && all.y) {
    type <- "right"
  } else {
    type <- "inner"
  }

  xy <- join(x, y, type, on, envir = NULL)

  if (type %chin% c("inner", "left")) {
    by <- lapply(by.xx, as.name)
    names(by) <- by.x
    by <- as.call(c(list(as.name("list")), by))
  } else if (type == "right") {
    by <- lapply(by.yy, as.name)
    names(by) <- by.x
    by <- as.call(c(list(as.name("list")), by))
  } else { #type is "outer"
    by <- list()
    for (i in seq_along(by.x)) {
      by[[i]] <- as.call(lapply(list("coalesce", by.xx[i], by.yy[i]), as.name))
    }
    names(by) <- by.x
    by <- as.call(c(list(as.name("list")), by))
  }

  non_by <- setdiff(names(xy), c(by.xx, by.yy))
  names(non_by) <- non_by
  non_by <- sapply(non_by, as.name, simplify = FALSE)

  j <- as.call(c(as.list(by), non_by))
  j <- sub_lang(j, dbi_table = xy, specials = NULL)

  handle_j(xy, j, NULL)
}
