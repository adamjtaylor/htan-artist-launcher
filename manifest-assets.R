library(tidyverse, warn.conflicts = FALSE, quietly = TRUE)
library(lubridate, quietly = TRUE)

args <- R.utils::commandArgs(
    trailingOnly = TRUE,
    asValues = TRUE
    )


cloudflare_prefix = "https://d3p249wtgzkn5u.cloudfront.net"

manifest <- read_csv(args$manifest, show_col_types = FALSE) %>%
    rename(synid = synapseId) %>%
    select(synid, minerva, thumbnail) %>%
    pivot_longer(
        -synid,
        names_to = "type",
        values_to = "manifest_uri"
    )


assets <- read_csv(args$assets, show_col_types = FALSE) %>%
    mutate(modified = as_datetime(modified)) %>%
    #group_by(synid, type) %>%
    #slice_max(modified) %>%
    #ungroup() %>%
    select(synid, type, version, s3_uri, modified) %>%
    rename(bucket_uri = s3_uri)

known_issues <- read_csv(
    args$skip, 
    col_names = "synid",
    show_col_types = FALSE
    )

# If minerva story is empty check if it exists
updated_manifest <- manifest %>%
    left_join(assets, by = c("synid", "type")) %>%
    group_by(synid, type) %>%
    filter(modified == max(modified) | is.na(modified)) %>%
    ungroup() %>%
    mutate(
        manifest_uri = ifelse(
            is.na(manifest_uri), 
            "empty", 
            manifest_uri),
        manifest_uri = case_when(
            manifest_uri != bucket_uri ~ bucket_uri
            ),
    ) %>%
    select(synid, type,manifest_uri) %>%
    pivot_wider(
        names_from = type, 
        values_from = manifest_uri
    )

write_csv(updated_manifest, paste0("tmp/updated-", args$manifest))
    
    
#updated_manifest %>%
#    filter(is.na(thumbnail) & is.na(minerva)) %>%
#    filter(!synid %in% known_issues$synid ) %>%
#    select(synid) %>%
#    write_csv("tmp/makeboth.csv", col_names = FALSE)
#
#updated_manifest %>%
#    filter(is.na(thumbnail) & !is.na(minerva)) %>%
#    filter(!synid %in% known_issues$synid ) %>%
#    select(synid) %>%
#    write_csv("tmp/makethumbnail.csv", col_names = FALSE)
#
#updated_manifest %>%
#    filter(!is.na(thumbnail) & is.na(minerva)) %>%
#    filter(!synid %in% known_issues$synid ) %>%
#    select(synid) %>%
#    write_csv("tmp/makeminerva.csv", col_names = FALSE)

updated_manifest %>%
    filter(!synid %in% known_issues$synid ) %>%
    select(synid) %>%
    write_csv("tmp/all.csv", col_names = FALSE)