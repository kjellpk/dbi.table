session <- new.env(parent = emptyenv())
session$table_name_counter <- 0L
session$print <- NULL

KEY_BASE <- "+xcwrl+"
TMP_BASE <- "DBI_TEMPORARY_TABLE_"
