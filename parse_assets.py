import pandas as pd
import json
import re

# Reading the TSV file
data = pd.read_csv('./assets/htan-assets-bucket.tsv', sep='\t', names=["bucket", "key", "modified"])

# Filtering rows
data = data[data['key'].str.contains('synid/syn') & data['key'].str.contains('thumbnail\.png|jpg|index\.html$')]

data = data[~data['key'].str.contains('nasty_ekeblad', na=False)]


# Extracting new columns
data[['synid', 'version', 'type']] = data['key'].str.extract(r'synid/(syn\d{8})/(.+?)/((?:thumbnail(?:\.png|\.jpg)|minerva/index\.html)?)', expand=True)

data = data[pd.notna(data['type'])]

data['type'] = data['type'].map({
    'minerva/index.html': 'minerva',
    'thumbnail.png': 'thumbnail',
    'thumbnail.jpg': 'thumbnail'
}, na_action='ignore')

# Adding new columns for URLs
data['s3_uri'] = 's3://' + data['bucket'] + '/' + data['key']
data['cloudfront_url'] = 'https://d3p249wtgzkn5u.cloudfront.net/' + data['key']


# Writing the processed data to a CSV file
data.to_csv('./assets/htan-assets-tidy.csv', index=False)

# Identifying the latest entries
latest = data.groupby(['synid', 'type']).apply(lambda x: x[x['modified'] == x['modified'].max()])

# Writing the latest entries to a CSV file
latest.to_csv('./assets/htan-assets-latest.csv', index=False)

# Additional transformations for JSON output
latest = latest.reset_index(drop=True)
latest = latest.drop(['s3_uri', 'version', 'modified', 'bucket', 'key'], axis=1)
latest_json = latest.pivot_table(index=['synid'], columns='type', values='cloudfront_url', aggfunc='first').reset_index()


# Writing to a JSON file
with open('assets/htan-imaging-assets.json', 'w', encoding='utf-8') as f:
    json.dump(latest_json.to_dict(orient='records'), f, ensure_ascii=False, indent=4)
