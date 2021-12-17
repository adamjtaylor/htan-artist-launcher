# HTAN Artist Launcher

A launcher for htan artist

1. Add your Tower access token, workspace name and AWS profile into the .token file
2. Ensure your profile is logged in: `aws sso login --profile <profile>`
3. Run `make all`

This will run the following process:

- `list`: List all files in the `htan-artist` bucket and tidy to just show the `thumbnail.png` and `minerva/index.html` files
- `map`: Read the manifest, check if assets have already been generated, if there is a newer version add this to an updated manifest. If there is no asset add to the list to generate
- `stage`: Copy the csv of synids that need generation to the staging folder of the `htan-project-tower-bucket`
- `launch`: Launch a the `htan-artist` pipleine on Tower with the latest queue file
- `check`: Check that the run is submitted and save some run details
- `archive`: Move files from `tmp` into the `logs` dir in a subdirectory named for the `workflowId`