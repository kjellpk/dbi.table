for (n in names(memory_connections)) {
  conn <- memory_connections[[n]]

  test_that(paste0("as.dbi.table works", " [", n, "]"), {
    expect_s3_class(as.dbi.table(conn, datasets::iris), "dbi.table")
  })

  # type = 'auto' is default so should be same as above
  test_that(paste0("as.dbi.table works when type = 'auto'", " [", n, "]"), {
    expect_s3_class(
      as.dbi.table(conn, datasets::iris, type = "auto"),
      "dbi.table"
    )
  })

  test_that(paste0("as.dbi.table works when type = 'query'", " [", n, "]"), {
    expect_s3_class(
      as.dbi.table(conn, datasets::iris, type = "query"),
      "dbi.table"
    )
  })

  test_that(paste0("as.dbi.table works when type = 'temp'", " [", n, "]"), {
    expect_s3_class(
      as.dbi.table(conn, datasets::iris, type = "temporary"),
      "dbi.table"
    )
  })

  test_that(paste0("as.dbi.table preserves keys", " [", n, "]"), {
    idt <- as.data.table(datasets::iris)
    idt[, pk := seq_len(.N)]
    setkeyv(idt, "pk")

    x <- as.dbi.table(conn, idt)
    expect_identical(key(x), "pk")

    x <- as.dbi.table(conn, idt, type = "auto")
    expect_identical(key(x), "pk")

    x <- as.dbi.table(conn, idt, type = "query")
    expect_identical(key(x), "pk")

    x <- as.dbi.table(conn, idt, type = "temporary")
    expect_identical(key(x), "pk")
  })
}
