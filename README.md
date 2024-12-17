## HTAN Artist Pipeline Launcher
A tool for launching the HTAN Artist pipeline on Seqera Platform to generate thumbnails and Minerva stories for imaging data.

### Overview
This tool:

- Queries BigQuery to identify HTAN imaging files that need thumbnails or Minerva stories
- Generates a samplesheet of files to process
- Launches the nf-artist pipeline on Seqera Platform

### Prerequisites
- Python 3.7+
- Google Cloud credentials configured for BigQuery access
- Seqera Platform credentials configured

### Shortlisting Logic
The shortlisting logic involves the following steps:

1. Fetch the latest HTAN imaging assets from a specified URL.
2. Identify files that already have thumbnails or Minerva stories.
3. Query Google BigQuery to get a list of imaging files from the `htan-dcc` project.
4. Filter out files that do not need thumbnails or Minerva stories.
5. Generate a samplesheet of files that need processing.

### Usage
1. Generate the samplesheet:  

    ```
    python shortlist.py
    ```  
    
    This will:
    - Query BigQuery for HTAN imaging files
    - Check which files need thumbnails/Minerva stories
    - Create samplesheets in samplesheet

2. Launch the pipeline:
    ```
    python launch.py
    ```
    This will:

    - Upload the samplesheet to Seqera Platform
    - Launch the nf-artist pipeline on Seqera Platform