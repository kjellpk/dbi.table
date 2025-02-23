for (n in names(chinook_connections)) {
  conn <- chinook_connections[[n]]

  test_that(paste0("dbi.catalog works ", " [", n, "]"), {
    expect_no_error(chinook <- dbi.catalog(conn))

    # is_dbi_catalog is not exported
    expect_true(is_dbi_catalog(chinook))

    # is_dbi_schema is not exported
    expect_true(is_dbi_schema(chinook$main))

    expect_true(is.dbi.table(chinook$main$Album))
  })
}
