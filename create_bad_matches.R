library(dplyr, warn.conflicts = FALSE)
library(DBI)
library(googlesheets4)

pg <- dbConnect(RPostgres::Postgres())
rs <- dbExecute(pg, "SET search_path TO streetevents")
calls <- tbl(pg, "calls")
crsp_link <- tbl(pg, "crsp_link")

regex <- "(?:Earnings(?: Conference Call)?|Financial and Operating Results|Financial Results Call|"
regex <- paste0(regex, "Results Conference Call|Analyst Meeting)")
regex <- paste0("^(.*) (", regex, ")")
qtr_regex <- "(Preliminary Half Year|Full Year|Q[1-4])"
year_regex <- "(20[0-9]{2}(?:-[0-9]{2}|/20[0-9]{2})?)"
period_regex <- paste0("^", qtr_regex, " ", year_regex," (.*)")

calls_mod <- 
    calls %>% 
    mutate(fisc_qtr_data = regexp_matches(event_title, period_regex)) %>%
    mutate(event_co_name = sql("fisc_qtr_data[3]")) %>%
    mutate(event_co_name = regexp_matches(event_co_name, regex)) %>%
    mutate(event_co_name = sql("event_co_name[1]")) %>% 
    select(file_name, event_title, event_co_name) %>% 
    inner_join(crsp_link %>% # file_name, permno
                   filter(!is.na(permno)), 
               by = "file_name") %>% 
    select(-match_type, - match_type_desc) %>% 
    distinct() %>% 
    collect()

name_checks <- read_sheet("1_RKRJah6iuUHC-y_kHP58Dl6UaptIhBNPRSyTjIjSSM")


dbExecute(pg, "DROP TABLE IF EXISTS bad_matches")

bad_matches <- 
    name_checks %>% 
    filter(!valid) %>% 
    inner_join(calls_mod, by = c("event_co_name", "permno")) %>% 
    select(file_name, permno, valid, event_co_name, comnams, event_title) %>% 
    copy_to(pg, ., name = 'bad_matches', temporary = FALSE)

dbExecute(pg, "ALTER TABLE .bad_matches OWNER TO streetevents")
dbExecute(pg, "GRANT SELECT ON bad_matches TO streetevents_access")
db_comment <- paste0("CREATED USING create_bad_matches.R ON ", Sys.time())
dbExecute(pg, sprintf("COMMENT ON TABLE bad_matches IS '%s';", db_comment))
