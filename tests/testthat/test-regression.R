for (n in names(memory_connections)) {
  conn <- memory_connections[[n]]

  test_that(paste0("issue 4: 'on = .(name = name2)' works in extract", " [", n, "]"), {
    test <- data.table(name = c("Sam", "Tom", "Lee"),
                       age = c(25, 35, 20))
    test2 <- data.table(name2 = c("Sam", "Tom", "Lee"),
                        job = c("electrician", "plumber", "scientist"))

    table1 <- as.dbi.table(conn, test, type = "temporary")
    table2 <- as.dbi.table(conn, test2, type = "temporary")

    expect_no_error(table1[table2, on = .(name = name2)])
  })


  test_that(paste0("dbi.table evaluates $, [[, and [", " [", n, "]"), {
    x <- as.dbi.table(conn, data.frame(u = 1))
    tmp <- list(v = 1)

    expect_true(reference.test({
      x[u == tmp$v]
    }))

    expect_true(reference.test({
      x[u == tmp[["v"]]]
    }))

    expect_true(reference.test({
      x[u == tmp["v"]]
    }))
  })
}
