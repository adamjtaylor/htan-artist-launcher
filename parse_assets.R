library(tidyverse)
library(jsonlite)

data = read_tsv('./assets/htan-assets-bucket.tsv', col_names = c("bucket", "key", "modified")) %>%
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

write_csv(data, './assets/htan-assets-tidy.csv')

latest <- data %>%
   group_by(synid, type) %>%
    filter(modified == max(modified))
    
write_csv(latest, "assets/htan-assets-latest.csv")

latest %>%
    ungroup() %>%
    select(-s3_uri,-version, -modified,-bucket,-key) %>%
    pivot_wider(names_from = type, values_from = cloudfront_url) %>%
    jsonlite::write_json('assets/htan-assets-latest.json')

