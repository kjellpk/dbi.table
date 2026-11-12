relational_merge <- function(x, recursive) {
  fkl <- attr(x, "foreign_keys", exact = TRUE)
  if (recursive == 0L || length(fkl) == 0L) return(x)
  catalog <- get_catalog(x)

  for (fk in fkl) {
    y <- dbi_table_from_id_key(catalog, fk$referenced_id_key)
    y <- relational_merge(y, recursive - 1L)

    by_x <- fk$foreign_key
    nm <- paste(by_x, collapse = "___")
    names(y) <- paste(nm, names(y), sep = ".")
    by_y <- paste(nm, fk$primary_key, sep = ".")

    x <- merge(x, y, by.x = by_x, by.y = by_y, all.x = TRUE,
               suffixes = rep("", 2L))
  }

  x
}
