library(tidyverse)

transfer_log <- read_csv('tmp/transfer.log', col_names = "log_entry")
succeeded <- transfer_log %>%
    extract(
        log_entry,
        into = c("from", "to"),
        regex = "(s3.*)\ to\ (s3.*$)"
    ) %>%
    filter(
        str_detect(from, "thumbnail.png") | 
        str_detect(from, "minerva/index.html")
    ) %>%
    mutate(
        type = str_extract(from, "thumbnail|minerva")
    ) %>%
    mutate(synid = str_extract(from, "syn\\d{8}")) %>%
    select(synid, type, from, to) 
    
succeeded %>% write_csv("tmp/succeeded.csv")

submitted <- read_csv('tmp/all.csv', col_names = "synid")
success_fail_log <- succeeded %>%
    select(synid, type) %>%
    mutate(generated = "succeeded") %>%
    pivot_wider(
        names_from = type,
        values_from = generated
    ) %>%
    mutate_all(replace_na, "failed") %>%
    full_join(submitted) %>%
    mutate_all(replace_na, "failed") %>%
    write_csv("tmp/success_fail_log.csv")

success_fail_log %>% 
    count(minerva, thumbnail)
