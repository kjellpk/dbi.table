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

  names(x) <- idx <- paste0("x", seq_len(length(nx)))
  names(y) <- idy <- paste0("y", seq_len(length(ny)))

  names(idx) <- nx
  names(idy) <- ny

  by.x <- idx[by.x]
  by.y <- idy[by.y]

  on <- lapply(paste(by.y, by.x, sep = " == "), str2lang)
  on <- handy_andy(on)

  type <- if (!length(by.x)) {
            "cross"
          } else if (all.x && all.y) {
            "outer"
          } else if (all.x && !all.y) {
            "left"
          } else if (!all.x && all.y) {
            "right"
          } else {
            "inner"
          }

  xy <- join(x, y, type, on, emptyenv())

  if (type %chin% c("inner", "left")) {
    by <- lapply(by.x, as.name)
  } else if (type == "right") {
    by <- lapply(by.y, as.name)
  } else { #type is "outer"
    by <- list()
    for (i in seq_along(by.x)) {
      by[[i]] <- as.call(lapply(list("coalesce", by.x[i], by.y[i]), as.name))
    }
  }

  by <- lapply(by, sub_lang, remotes = c(xy))
  names(by) <- names(idx)[match(by.x, idx)]

  xb <- c(xy)[setdiff(names(x), by.x)]
  nxb <- names(idx)[match(names(xb), idx)]

  yb <- c(xy)[setdiff(names(y), by.y)]
  nyb <- names(idy)[match(names(yb), idy)]

  dups <- intersect(nxb, nyb)
  nxb[nxb %chin% dups] <- paste0(nxb[nxb %chin% dups], suffixes[1L])
  nyb[nyb %chin% dups] <- paste0(nyb[nyb %chin% dups], suffixes[2L])

  names(xb) <- nxb
  names(yb) <- nyb

  a <- attributes(xy)
  xy <- c(by, xb, yb)
  a$names <- names(xy)
  attributes(xy) <- a

  xy
}
