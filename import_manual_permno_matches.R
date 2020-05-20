library(googlesheets4)
library(dplyr, warn.conflicts = FALSE)
library(DBI)

# You may need to run gs_auth() to set this up
gs <- "14F6zjJQZRsf5PonOfZ0GJrYubvx5e_eHMV_hCGe42Qg"

permnos_addl <-
    read_sheet(gs, sheet = "match_repair.csv") %>%
    filter(!same_permco) %>%
    select(file_name, permno, co_name) %>%
    mutate(comment = "Cases resolved using company names in 2017")

diff_permcos <- read_sheet(gs, sheet = "diff_permcos") 

add_man_matches <-
    diff_permcos %>%
    filter(correct != 'DN') %>%
    mutate(permno = case_when(correct == 'Y' ~ permno.y,
                              correct == 'X' ~ permno.x,
                              correct == 'Z' ~ permno.z,
                              correct == 'NONE' ~ NA_real_),
           co_name = NA_character_) %>%
    select(file_name, permno, co_name) %>%
    mutate(comment = "Cases resolved using company names in 2020")

permnos <-
    read_sheet(gs, sheet = "manual_permno_matches") %>%
    select(file_name, permno, co_name, comment) %>%
    union(permnos_addl) %>%
    union(add_man_matches)

pg_comment <- function(table, comment) {
    sql <- paste0("COMMENT ON TABLE ", table, " IS '",
                  comment, " ON ", Sys.time() , "'")
    rs <- dbExecute(pg, sql)
}

pg <- dbConnect(RPostgres::Postgres())

dbExecute(pg, "SET search_path TO se_links")

rs <- dbWriteTable(pg, "manual_permno_matches",
                   permnos,
                   overwrite=TRUE, row.names=FALSE)

rs <- dbExecute(pg, "ALTER TABLE manual_permno_matches OWNER TO se_links_access")

rs <- dbExecute(pg,
    "DELETE FROM manual_permno_matches
    WHERE file_name IN (
        SELECT file_name
        FROM manual_permno_matches
        GROUP BY file_name
        HAVING count(DISTINCT permno)>1)
            AND comment != 'Fix by Nastia/Vincent in January 2015'")

rs <- dbExecute(pg, "CREATE INDEX ON manual_permno_matches (file_name)")

rs <- dbExecute(pg, "ALTER TABLE manual_permno_matches OWNER TO se_links")

rs <- dbExecute(pg, "GRANT SELECT ON manual_permno_matches TO se_links_access")

rs <- pg_comment("manual_permno_matches",
                 paste0("CREATED USING import_manual_permno_matches.R ON ", Sys.time()))

rs <- dbDisconnect(pg)

