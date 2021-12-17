NAME=htan-asset-mapping
include .token

all: list map stage launch check archive

list:
# List all the objects in the htan-assets bucket
# Parse them to pick just thumbnails and minerva/index.html files

	python generate_bucket_manifest.py \
		-b htan-assets \
		-t aws \
		-p htan-dev \
		> assets/htan-assets-bucket.tsv
	Rscript parse_assets.R

map:
# Read a synapse manifest, get columns synapseId, minerva and thubmnail
# For each entry in minerva and thumbnail columns check that it exists in the bucket listing
# For each entry test the cloudflare url response
# If any of these fails split out into a report file
# For all rows check the bucket manifest for an entry and update if there is a more recent version
# Export a list of synapse Ids that do not have an entry

# a list that need thumbnails
# a list that needs minerva stories
	Rscript manifest-assets.R \
		--manifest manifest.csv \
		--assets assets/htan-assets-tidy.csv \
		--skip skiplist.csv

stage:
# Copy the queue to s3
	aws s3 cp \
		--profile sandbox-developer \
		tmp/all.csv \
		s3://htan-project-tower-bucket/staging/queue.csv

launch:
# Launch the pipeline
	tw \
		--access-token=${TOKEN} \
		--url=https://tower.sagebionetworks.org/api \
		--output=json \
		launch htan-artist \
		--workspace=${WORKSPACEID} \
		--params-file=params.yaml \
		> tmp/launched.json


check: 
# Check it has been submitted and save details
	workflowId=$$(cat 'tmp/launched.json' | jq -r ".workflowId"); \
	tw \
		--access-token=${TOKEN} \
		--url=https://tower.sagebionetworks.org/api \
		--output=json \
		runs view \
		--id=$$workflowId \
		--workspace=${WORKSPACEID} \
		> tmp/runview.json

archive:
# Archive into a folder named by workflowId
	workflowId=$$(cat 'tmp/launched.json' | jq -r ".workflowId"); \
	mkdir logs/$${workflowId}; \
	mv tmp/* logs/$${workflowId}/

