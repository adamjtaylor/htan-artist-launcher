import requests
import json
import pandas as pd


url = (
    "https://d3p249wtgzkn5u.cloudfront.net/final-output/htan-imaging-assets-latest.json"
)
try:
    response = requests.get(url)
    response.raise_for_status()
    data = json.loads(response.text)
except requests.exceptions.RequestException as e:
    print(f"Error: Unable to fetch data from the URL. {e}")
    data = []

# List synids that do not have a thumbnail

has_minerva = []
has_thumbnail = []

for item in data:
    synid = item.get("synid")
    if "thumbnail" in item:
        has_thumbnail.append(synid)
    if "minerva" in item:
        has_minerva.append(synid)

print(f"Number of synids with a thumbnail: {len(has_thumbnail)}")
print(f"Number of synids with a minerva: {len(has_minerva)}")

# Using google big query to get a list of files to release. Proiject is htan-dcc

from google.cloud import bigquery

client = bigquery.Client()

query = """
SELECT 
    e.entityId as id, 
    i2.Cloud_Storage_Path AS image,
    i2.Imaging_Assay_Type = 'H&E' AS he, 
    CASE
        WHEN REGEXP_CONTAINS(i2.Filename, r'\.ome\.tif{1,2}$') THEN FALSE
        ELSE TRUE
    END AS convert,
    i2.Imaging_Assay_Type AS type,
    i2. HTAN_Center AS center
FROM `htan-dcc.released.entities` e
LEFT JOIN `htan-dcc.combined_assays.ImagingLevel2` i2
ON e.entityId = i2.entityId
WHERE e.Component = 'ImagingLevel2' AND i2.Imaging_Assay_Type != 'MERFISH'
AND i2.HTAN_Center != 'HTAN HTAPP'
"""

query_job = client.query(query)
results = query_job.result()
df = results.to_dataframe()

df["miniature"] = ~df["id"].isin(has_thumbnail)
df["minerva"] = ~df["id"].isin(has_minerva)

# extrract cloud provider from image path eg s3/gs
df["cloud_provider"] = df["image"].str.extract(r"(s3|gs)://")

# Filter for s3 only

df = df[df["cloud_provider"] == "s3"]


# Filter for where at least one of minerva or thumbnail is set to True

df = df[(df["miniature"] | df["minerva"])]

print(f"Number of files needing thumbnail or minerva: {len(df)}")
print(f'Number of files needing thumbnail: {len(df[df["miniature"]])}')
print(f'Number of files needing minerva: {len(df[df["minerva"]])}')

# Sort by highest synid

df = df.sort_values(by="id", ascending=False)
print(df.sample(5))

# Count by type

print(df["type"].value_counts())
print(df["center"].value_counts())


# Send to Seqera Platform as a dataset

df.to_csv("samplesheet/artist-samplesheet.csv", index=False)

df_sample10 = df.sample(10)

df_sample10.to_csv("samplesheet/artist-samplesheet-sample10.csv", index=False)
print("Dataset saved as samplesheet/artist-samplesheet.csv")
