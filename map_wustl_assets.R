
old_portal <- "https://raw.githubusercontent.com/ncihtan/htan-portal/f0f9c79174f8df3a3660b9cd475cb18066c9db41/public/processed_syn_data.json" %>%
    jsonlite::read_json()

old_portal_images <- old_portal$files %>% 
    enframe() %>% unnest_wider(value) %>%
    filter(Component == "ImagingLevel2")

old_wustl <- old_portal_images %>% 
    filter(atlas_name == "HTAN WUSTL") %>%
    mutate(old_synid = synapseId) %>%
    select(filename, old_synid) %>%
    mutate(basename = basename(filename)) %>%
    select(-filename)

old_wustl

new_wustl <- portal_images %>%
    filter(atlas_name == "HTAN WUSTL") %>%
    mutate(new_synid = synapseId) %>%
    select(filename, new_synid) %>%
    mutate(basename = basename(filename)) 

new_wustl %>%
    full_join(old_wustl) %>%
    rename(synid = old_synid) %>%
    left_join(latest) %>%
    mutate(
        key = str_remove(key, "index.html"),
        new_key = str_replace(key, synid, new_synid)
        ) %>%
    select(bucket,key, new_key) %>%
    na.omit() %>%
    mutate(cmd = glue::glue("aws s3 mv --profile htan-dev --recursive --dryrun s3://{bucket}/{key} s3://{bucket}/{new_key}"),
    ) %>% select(cmd) %>%
    write_csv("move_wustl/Makefile", col_names = FALSE)
