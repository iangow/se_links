# Create_crsp_link.R
library(dplyr, warn.conflicts = FALSE)
library(DBI)
library(readr)

pg <- dbConnect(RPostgreSQL::PostgreSQL())

rs <- dbGetQuery(pg, read_file('crsp_link.sql'))

db_comment <- paste0("CREATED USING create_crsp_link.R ON ", Sys.time())
rs <- dbExecute(pg, sprintf("COMMENT ON TABLE streetevents.crsp_link IS '%s';", db_comment))
