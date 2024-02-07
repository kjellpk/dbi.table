#' @export
merge.dbi.table <- function(x, y, by = NULL, by.x = NULL, by.y = NULL,
                            all = FALSE, all.x = all, all.y = all,
                            sort = FALSE, suffixes = c(".x", ".y"),
                            no.dups = TRUE, ...) {
  if (!is.dbi.table(x)) {
    stop(sQuote("x"), " is not a ", sQuote("dbi.table"))
  }

  if (!is.dbi.table(y)) {
    stop(sQuote("y"), " is not a ", sQuote("dbi.table"))
  }

  names_x <- names(x)
  names_y <- names(y)

  if (anyDuplicated(names_x) || anyDuplicated(names_y)) {
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
      stop("non-empty character vectors of column names are required for ",
           sQuote("by.x"), " and ", sQuote("by.y"))

    if (!all(by.x %chin% names_x))
      stop("Elements listed in ", sQuote("by.x"),
           " must be valid column names in ",
           sQuote("x"))

    if (!all(by.y %chin% names_y))
      stop("Elements listed in ", sQuote("by.y"),
           " must be valid column names in ",
           sQuote("y"))
  } else {
    if (!length(by)) {
      stop("a non-empty character vector of column names is required for ",
           sQuote("by"))
    }

    if (!all(by %chin% intersect(names_x, names_y)))
      stop("Elements listed in ", sQuote("by"),
           " must be valid column names in ",
           sQuote("x"), " and ", sQuote("y"))

    by <- unname(by)
    by.x <- by.y <- by
  }

  on <- paste(paste0("x.", by.x), paste0("y.", by.y), sep = " == ")
  on <- handy_andy(lapply(on, str2lang))

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

  x_length <- length(x)
  by_x_jdx <- chmatch(by.x, names_x)
  x_non_by_jdx <- setdiff(seq_along(x), by_x_jdx)
  by_y_jdx <- chmatch(by.y, names_y)
  y_non_by_jdx <- setdiff(seq_along(y), by_y_jdx)

  non_by <- names(xy)[c(x_non_by_jdx, x_length + y_non_by_jdx)]
  non_by <- unname(sapply(non_by, as.name, simplify = FALSE))

  if (type %chin% c("inner", "left")) {
    by <- names_list(xy)[by_x_jdx]
  } else if (type == "right") {
    by <- names_list(xy)[x_length + by_y_jdx]
  } else if (type == "outer") {
    by_x <- sapply(names(xy)[by_x_jdx], as.name, simplify = FALSE)
    by_y <- sapply(names(xy)[x_length + by_y_jdx], as.name, simplify = FALSE)
    by <- unname(mapply(call, name = "coalesce", by_x, by_y))
  } else { #for cross joins
    by <- list()
  }

  j <- c(c(by, non_by))

  # naming logical taken from merge.data.table (data.table version 1.14.10)
  start <- setdiff(names_x, by.x)
  end <- setdiff(names_y, by.y)
  dupnames <- intersect(start, end)
  if (length(dupnames)) {
    start[chmatch(dupnames, start, 0L)] <- paste0(dupnames, suffixes[1L])
    end[chmatch(dupnames, end, 0L)] <- paste0(dupnames, suffixes[2L])
  }
  dupkeyx <- intersect(by.x, end)
  if (no.dups && length(dupkeyx)) {
    end[chmatch(dupkeyx, end, 0L)] <- paste0(dupkeyx, suffixes[2L])
  }

  names(j) <- c(by.x, start, end)
  handle_j(xy, j, by = NULL, enclos = NULL)
}
