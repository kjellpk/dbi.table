% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/attach.R
\name{dbi.attach}
\alias{dbi.attach}
\title{Attach a Database Schema to the Search Path}
\usage{
dbi.attach(
  what,
  pos = 2L,
  name = NULL,
  warn.conflicts = FALSE,
  schema = NULL,
  graphics = TRUE
)
}
\arguments{
\item{what}{a connection handle returned by \code{\link[DBI]{dbConnect}} or a
zero-argument function that returns a connection handle.}

\item{pos}{an integer specifying position in \code{\link[base]{search}}() where to
attach.}

\item{name}{a character string specifying the name to use for the attached database.}

\item{warn.conflicts}{a logical value. If \code{TRUE}, warnings are printed about
\code{\link[base]{conflicts}} from attaching the database, unless that
database contains an object \code{.conflicts.OK}. A conflict is a function
masking a function, or a non-function masking a non-function.}

\item{schema}{a character string specifying the name of the schema to attach.}

\item{graphics}{a logical value; passed to \code{\link[utils]{menu}}. In interactive
sessions, when \code{schema} is \code{NULL} and multiple schemas are
found on \code{what}, a menu is displayed to select a schema.}
}
\value{
an \code{\link{environment}}, the attached schema is invisibly returned.
}
\description{
The database schema is attached to the R search path. This means that the
  schema is searched by R when evaluating a variable, so that
  \code{\link{dbi.table}}s in the schema can be accessed by simply giving
  their names.
}
\seealso{
\code{\link[base]{attach}}
}
