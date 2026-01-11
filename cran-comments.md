# 1.0.6 Changes Relative to Version 1.0.5 (Current CRAN Version)

- New feature: added support for lists of the form `.(name = symbol)` to the
  `on` argument in the extract (`[`) method.


## Test Environments

(Github Actions)

* macos_latest (release)
* windows-latest (release)
* ubuntu-latest (devel)
* ubuntu-latest (release)
* ubuntu-latest (oldrel-1)

# Relevant Notes from Previous CRAN Submissions

## The `dbi.attach` Function Calls `attach`

One of the main features of `dbi.table` is the ability to attach database
schemas to the search path; `dbi.table::dbi.attach` thus calls `attach`.
To avoid Errors being thrown in R CMD check, this call to `attach` is
implemented as follows.
```
  e <- get("attach", "package:base")(what, pos = pos, name = name,
                                     warn.conflicts = warn.conflicts)
```

`dbi.attach` will not attach the same connection multiple times unless the user
provides a distinct `name` in each subsequent call to `dbi.attach`.


## Writing to the User's Home Filespace

Possbile spurious warning.

The package only creates files in the path returned by `tempdir`. Some databases
(e.g., duckdb) support only a single open read/write connection. Subsequent
calls to `chinook.duckdb()` need to return working DBI connections. There is a
constraint that the basename of the database file name needs to be the name of
the database catalog (that is, all the database files have to have the same
basename). My solution is to put each database file in a separate temporary
directory. These are created as follows, perhaps calling `tempfile` inside
`dir.create` is throwing the false positive.

```{r}
temp_db_path <- function(db_file_name) {
  if (!dir.create(tmp_path <- tempfile("ex"))) {
    stop("could not create directory ", tmp_path)
  }

  file.path(tmp_path, db_file_name)
}
```

## References Describing the Methods in your Package

There are no references other than the package vignette.


---

# Comments for Previous CRAN Submissions

# 1.0.4 Changes Relative to Version 1.0.3

- Moved inline SQL statements in .R files to .sql files in inst/sql_statements.

- Added support for more RDBMSs. Improved foreign key lookup and merging.


# 1.0.3 Changes Relative to Version 1.0.1

- At the request of the data.table maintainers, moved data.table from Depends
  to Suggests.

- Added Luis Rocha as a contributor (ctd) for the (Chinook) example database
  and included his license in example_files/chinook_export/LICENSE.

- Refactored code; no changes to core functionality of package.


# 1.0.1 Initial Package Submission (2024-11-29)

This resubmission of the `dbi.table` package includes the changes requested by
K. Lauseker from my first submission attempt. The requested changes and my
actions are in section 1.0.1.

Also, the package uses the `attach` function. See 1.0.0 (at the very end) for
details.


# Test Environments

(Github Actions)

* macos_latest (release)
* windows-latest (release)
* ubuntu-latest (devel)
* ubuntu-latest (release)
* ubuntu-latest (oldrel-1)


## Requested Changes for Initial Submission

The following changes were requested by K. Lauseker (2024-12-02).

- Please use only undirected quotation marks in the description text. e.g.
  `data.table` --> 'data.table'
  - FIXED: changed `data.table` to 'data.table' in description.


- Please always write package names, software names and API (application
  programming interface) names in single quotes in title and description.
  e.g: --> 'DBI'
  Please note that package names are case sensitive.
  For more details:
  <https://contributor.r-project.org/cran-cookbook/description_issues.html#formatting-software-names>
  - FIXED: changed DBI to 'DBI' in description; changed `data.table` to
           'data.table' in title.


- If there are references describing the methods in your package, please
  add these in the description field of your DESCRIPTION file in the form
  authors (year) <doi:...>
  authors (year, ISBN:...)
  or if those are not available: <https:...>
  with no space after 'doi:', 'https:' and angle brackets for
  auto-linking. (If you want to add a title as well please put it in
  quotes: "Title")
  For more details:
  <https://contributor.r-project.org/cran-cookbook/description_issues.html#references>
  - UNCHANGED: there are no references other than the package vignette.



- Please add \value to .Rd files regarding exported methods and explain
  the functions results in the documentation. Please write about the
  structure of the output (class) and also what the output means. (If a
  function does not return a value, please document that too, e.g.
  \value{No return value, called for side effects} or similar)
  For more details:
  <https://contributor.r-project.org/cran-cookbook/docs_issues.html#missing-value-tags-in-.rd-files>
  Missing Rd-tags:
  - FIXED: as.data.table.Rd: \value
  - FIXED: as.dbi.table.Rd: \value
  - FIXED: csql.Rd: \value
  - FIXED: dbi.attach.Rd: \value
  - FIXED: example_databases.Rd: \value
  - FIXED: reference.test.Rd: \value
  - FIXED: sql.join.Rd: \value



- Please ensure that your functions do not write by default or in your
  examples/vignettes/tests in the user's home filespace (including the
  package directory and getwd()). This is not allowed by CRAN policies.
  Please omit any default path in writing functions. In your
  examples/vignettes/tests you can write to tempdir().
  For more details:
  <https://contributor.r-project.org/cran-cookbook/code_issues.html#writing-files-and-directories-to-the-home-filespace>
  - SPURIOUS: R/examples.R

    The package only creates files in the path retuned
    by `tempdir`. Some databases (e.g., duckdb) support only a single open
    read/write connection. Subsequent calls to `chinook.duckdb()` need to return
    working DBI connections. There is a constraint that the basename of the
    database file name needs to be the name of the database catalog (that is,
    all the database files have to have the same basename). My solution is to
    put each database file in a separate temporary directory. These are created
    as follows, perhaps calling `tempfile` inside `dir.create` is throwing the
    false positive.
    ```{r}
    temp_db_path <- function(db_file_name) {
      if (!dir.create(tmp_path <- tempfile("ex"))) {
        stop("could not create directory ", tmp_path)
      }

    file.path(tmp_path, db_file_name)
    }
    ```
  
- Please always make sure to reset to user's options(), working directory
  or par() after you changed it in examples and vignettes and demos.
  e.g.:

  old <- options(width = 90)
  ...
  options(old)

  -> FIXED: inst/doc/introduction_to_dbi_table.R

  Added this chunk at the beginning of the vignette to set width:
  
  ```
  {r, set_options, include = FALSE}
  old_opts <- options(width = 90)
  ```

  And this chunk at the end to restore the original options.

  ```
  {r, restore_options, include = FALSE}
  options(old_opts)
  ```

  For more details:
  <https://contributor.r-project.org/cran-cookbook/code_issues.html#change-of-options-graphical-parameters-and-working-directory>


# 1.0.0 Use of `attach` in the `dbi.table` Package

One of the main features of the `dbi.table` is the ability to attach database
schemas to the search path; `dbi.table::dbi.attach` thus calls `attach`.
To avoid Errors being thrown in R CMD check, this call to `attach` is
implemented as follows.
```
  e <- get("attach", "package:base")(NULL, pos = pos, name = name,
           warn.conflicts = warn.conflicts)
```
