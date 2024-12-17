import logging

# Import the seqerakit package
from seqerakit import seqeraplatform

import pandas as pd
import string
import shortuuid

alphabet = string.ascii_lowercase + string.ascii_uppercase + string.digits
su = shortuuid.ShortUUID(alphabet=alphabet)


def shortuuid_random():
    return su.random(length=4)


logging.basicConfig(level=logging.DEBUG)

# Construct a new seqerakit SeqeraPlatform instance
tw = seqeraplatform.SeqeraPlatform()

# Customise the entries below as required
workspace = "253119656982040"  # htan-project
compute_env = "3k6bQPIqpII2nGlDMbc9Mv"  # htan-project-spot-v12
url = "https://tower.sagebionetworks.org/api"

# Specify a human-readable run name
run_name = "hello-world-seqerakit"

# Launch the 'hello-world' pipeline using the 'launch' method
# pipeline_run = tw.launch(
#     "--workspace",
#     workspace,
#     "--compute-env",
#     compute_env,
#     "--name",
#     run_name,
#     "--revision",
#     "master",
#     "--wait",
#     "SUBMITTED",
#     "https://github.com/nextflow-io/hello",
#     to_json=True,
# )

# Read the samplehseet
samplesheet = "samplesheet/artist-samplesheet.csv"

# Submit the df as a Sequera samplesheet

# Samplesheet name: "artist-samplesheet" with todays's date (eg 20241217) and a random string)
today = pd.Timestamp.now().strftime("%Y%m%d")
uuid = shortuuid_random()
samplesheet_name = f"artist_samplesheet"
run_name = f"artist_{uuid}"


upload_dataset = tw.datasets(
    "add",
    samplesheet,
    "--workspace",
    workspace,
    "--name",
    samplesheet_name,
    "--description",
    "Samplesheet for artist pipeline",
    "--overwrite",
    "--header",
    to_json=True,
)

dataset_url = tw.datasets(
    "url",
    "--id",
    upload_dataset["datasetId"],
    "--workspace",
    workspace,
    to_json=True,
)
# write a params.yaml
params = f"""
input: "{dataset_url['datasetUrl']}"
outdir: "s3://htan-project-tower-bucket/outputs/{run_name}"
"""

params_name = f"params/params_{uuid}.yaml"

with open(params_name, "w") as f:
    f.write(params)

pipeline_run = tw.launch(
    "--workspace",
    workspace,
    "--compute-env",
    compute_env,
    "--name",
    run_name,
    "--revision",
    "main",
    "--wait",
    "SUBMITTED",
    "https://github.com/Sage-Bionetworks-Workflows/nf-artist",
    "--params-file",
    params_name,
    to_json=True,
)

print(pipeline_run)
