library(dplyr, warn.conflicts = FALSE)
library(DBI)

pg <- dbConnect(RPostgres::Postgres())

rs <- dbExecute(pg, "SET search_path TO se_links, streetevents")

company_ids <- tbl(pg, "company_ids")
calls <- tbl(pg, "calls")
manual_permno_matches <- tbl(pg, "manual_permno_matches")

stocknames <- tbl(pg, sql("SELECT * FROM crsp.stocknames"))

crsp_link_auto <-
    company_ids %>%
    mutate(ncusip = substr(cusip, 1, 8)) %>%
    select(file_name, last_update, ncusip) %>%
    inner_join(stocknames, by = "ncusip") %>%
    select(file_name, last_update, permno) %>%
    distinct() %>%
    compute()

rs <- dbExecute(pg, "DROP TABLE IF EXISTS crsp_link_new")

crsp_link <-
    crsp_link_auto %>%
    left_join(manual_permno_matches, by = "file_name") %>%
    mutate(permno = coalesce(permno.y, permno.x)) %>%
    select(file_name, last_update, permno) %>%
    compute(name = "crsp_link_new", temporary = FALSE)

rs <- dbExecute(pg, "ALTER TABLE crsp_link_new OWNER TO se_links")
rs <- dbExecute(pg, "GRANT SELECT ON crsp_link_new TO se_links_access")

rs <- dbExecute(pg, "CREATE INDEX ON crsp_link (file_name)")

db_comment <- paste0("CREATED USING iangow/se_links/create_crsp_link_new.R ON ",
                     Sys.time())
rs <- dbExecute(pg, sprintf("COMMENT ON TABLE crsp_link_new IS '%s';", db_comment))