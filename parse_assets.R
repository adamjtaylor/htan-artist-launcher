library(tidyverse)
library(jsonlite)

data = read_tsv('./assets/htan-assets-bucket.tsv', col_names = c("bucket", "key", "modified")) %>%
    filter(
        grepl("synid\\/syn", key) & grepl("thumbnail.[png|jpg]|index.html$", key)
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

write_csv(data, './assets/htan-assets-tidy.csv')

latest <- data %>%
   group_by(synid, type) %>%
    filter(modified == max(modified))
    
write_csv(latest, "assets/htan-assets-latest.csv")


#inerva_thumbs <- read_csv('minerva-thumbs.csv') %>% rename(new = thumbnail)
#inerva_thumbs
#
#inerva_thumbs <- read_tsv('./assets/htan-assets-bucket.tsv', col_names = c("bucket", "key", "modified")) %>%
#   filter(
#       grepl("synid\\/syn", key) & grepl("_0_0.jpg$", key)
#   ) %>%
#   extract(
#       key,
#       into = c("synid", "version"),
#       regex = "synid\\/(syn\\d{8})\\/(.+)\\/",
#       remove = FALSE
#   ) %>%
#   mutate(
#       level = str_match(key, "(\\d+)(_0_0.jpg)$")[,2]
#   )
#
#inerva_thumbs

latest
#portal_images <- "https://github.com/ncihtan/htan-portal/raw/master/public/processed_syn_data.json.gz"
#
#temp <- tempfile()
#download.file(portal_images, temp)
#gzfile(temp, 'rt')
#portal_images <- jsonlite::read_json(temp)
#unlink(temp)
#
#portal_images <- portal_images$files %>% 
#enframe() %>% unnest_wider(value) %>% filter(Component == "ImagingLevel2")

latest %>%
    ungroup() %>%
    select(-s3_uri,-version, -modified,-bucket,-key) %>%
    pivot_wider(names_from = type, values_from = cloudfront_url) %>%
    jsonlite::write_json('assets/htan-imaging-assets.json', pretty = TRUE)

#latest %>%
#    ungroup() %>%
#    select(-s3_uri,-version, -modified,-bucket,-key) %>%
#    pivot_wider(names_from = type, values_from = cloudfront_url) %>%
#    left_join(minerva_thumbs, by = c('synid','minerva')) %>%
#    filter(!grepl("thirsty_mclean", thumbnail)) %>%
#    mutate(thumbnail = case_when(is.na(thumbnail)) ~ new, TRUE ~ thumbnail) %>%
#    select(-new)
#    jsonlite::write_json('assets/htan-imaging-assets-fixed.json', pretty = TRUE)
