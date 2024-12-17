library(tidyverse)
library(jsonlite)
library(aws.s3)
library(aws.signature)
library(rvest)

# Get inventory from s3
# For now we will download

#inventory_gz <- "/Users/ataylor/Downloads/435080c5-12dc-41af-a3a1-125994e712a8.csv.gz"

inventory_url <- system(
    "aws s3 ls --profile htan-dev s3://htan-assets/inventory/htan-assets/htan-assets-inventory/data/ | sort | tail -n 1",
    intern = TRUE
    ) %>% 
    str_extract("[[a-z][0-9][\\-]]+.csv.gz") %>% 
    paste0("https://d3p249wtgzkn5u.cloudfront.net/inventory/htan-assets/htan-assets-inventory/data/",.)

inventory_url %>%
    download.file(destfile = paste0('./assets/', basename(.)))


inventory_get

inventory <- inventory_url %>%
    basename() %>%
    paste0('./assets/', .) %>%
    R.utils::gunzip(
    remove = FALSE,
    overwrite = TRUE) %>% 
    read_csv(col_names = c("bucket","key","size","modified"))

# Pull out the assets we need
assets <- inventory %>%
    filter(
        grepl("synid\\/syn", key) & grepl("thumbnail\\.png$|index.html$", key)
    ) %>%
    extract(
        key,
        into = c("synid", "version", "type"),
        regex = "synid\\/(syn\\d{8})\\/(.+)\\/(thumbnail|minerva)",
        remove = FALSE
    ) %>%
    mutate(
        s3_uri = paste0("s3://", bucket, "/", key),
        cloudfront_url = paste0("https://d3p249wtgzkn5u.cloudfront.net/", key)
    )

# Find the latest versions
latest <- assets %>%
   group_by(synid, type) %>%
    filter(modified == max(modified)) %>%
    ungroup()
    
latest


latest %>%
    ungroup() %>%
    select(-s3_uri,-version, -modified,-bucket,-key) %>%
    pivot_wider(names_from = type, values_from = cloudfront_url) %>%
    jsonlite::write_json('assets/htan-imaging-assets.json', pretty = TRUE)


# List images on the portal

portal_images <- "https://github.com/ncihtan/htan-portal/raw/master/public/processed_syn_data.json.gz"

temp <- tempfile()
download.file(portal_images, temp)
gzfile(temp, 'rt')
portal <- jsonlite::read_json(temp)
unlink(temp)

portal_images <- portal$files %>% 
    enframe() %>% unnest_wider(value) %>%
    filter(Component == "ImagingLevel2")

latest %>% 
    mutate(on_portal = synid %in% portal_images$synapseId) %>%
    group_by(type) %>%
    count(on_portal)

missing <- portal_images %>% 
    distinct(atlas_name, synapseId) %>%
    rename(synid = synapseId) %>%
    left_join(latest) %>%
    select(atlas_name,synid,type) %>%
    mutate(missing = FALSE) %>%
    pivot_wider(
        names_from = type,
        values_from = missing
    ) %>%
    select(-`NA`) %>%
    mutate_at(c("minerva","thumbnail"),replace_na, TRUE) %>%
    filter(minerva == TRUE | thumbnail == TRUE)

missing %>%
    count(atlas_name, minerva,thumbnail) %>%
    janitor::adorn_totals()

missing

story <- inventory %>%
    filter(grepl("story.json$", key)) %>%
    extract(
        key,
        into = c("synid", "version", "type"),
        regex = "synid\\/(syn\\d{8})\\/(.+)\\/(thumbnail|minerva)",
        remove = FALSE
    ) %>%
    mutate(
        s3_uri = paste0("s3://", bucket, "/", key),
        cloudfront_url = paste0("https://d3p249wtgzkn5u.cloudfront.net/", key)
    ) %>%
    group_by(synid) %>%
     filter(modified == max(modified)) %>%
    ungroup() %>%
    filter(synid %in% portal_images$synapseId) %>%
    left_join(
        portal_images %>% rename(synid = synapseId) %>% select(atlas_name, filenam, synid)
    )


story %>% select(synid, filename, cloudfront_url) %>% write_csv('story_json_list.csv')
 
