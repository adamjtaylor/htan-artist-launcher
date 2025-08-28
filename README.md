# HTAN Artist Pipeline Launcher

A comprehensive toolkit for launching the [nf-artist](https://github.com/Sage-Bionetworks-Workflows/nf-artist) pipeline on Seqera Platform to generate thumbnails and Minerva stories for imaging data.

## 🎯 Overview

This toolkit provides two main scripts:

1. **`shortlist.py`** - Queries BigQuery and S3 to identify HTAN imaging files that need thumbnails or Minerva stories
2. **`launch.py`** - Uploads samplesheets to Seqera Platform and launches the nf-artist pipeline

### What it does:
- 🔍 Identifies imaging files missing thumbnails or Minerva visualizations
- 📋 Generates samplesheets for processing
- 🚀 Launches automated pipelines on Seqera Platform
- 🧹 Manages temporary files and cleanup

## 📋 Prerequisites

### Required Software
- **Python 3.8+** 
- **uv** or **pip** for package management
- **Google Cloud SDK** (for BigQuery access)
- **AWS CLI** (for S3 access)

### Required Credentials
- **Google Cloud credentials** configured for BigQuery access to `htan-dcc` project
- **AWS credentials** configured with profile `htan-dev` (or custom profile)
- **Seqera Platform credentials** configured (via seqerakit)

## 🛠️ Installation & Setup

### 1. Clone the Repository
```bash
git clone https://github.com/ncihtan/htan-artist-launcher.git
cd htan-artist-launcher
```

### 2. Set Up Python Virtual Environment

#### Option A: Using uv (Recommended)
```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create and activate virtual environment
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv pip install -r requirments.txt
```

#### Option B: Using pip
```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirments.txt
```

### 3. Compile Requirements (if modifying dependencies)

If you need to add or update dependencies:

#### Using uv:
```bash
# Edit requirments.in to add/remove packages
# Then compile to generate locked versions
uv pip compile requirments.in -o requirments.txt
```

#### Using pip-tools:
```bash
# Install pip-tools first
pip install pip-tools

# Compile requirements
pip-compile requirments.in
```

### 4. Configure Credentials

#### Google Cloud (BigQuery)
```bash
# Install and configure gcloud CLI
gcloud auth login
gcloud config set project htan-dcc

# Or set up service account
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
```

#### AWS (S3)
```bash
# Configure AWS CLI with htan-dev profile
aws configure --profile htan-dev
# Enter your AWS Access Key ID, Secret Access Key, and region

# Or set up in ~/.aws/credentials:
[htan-dev]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY
region = us-east-1
```

#### Seqera Platform
```bash
# Configure seqerakit (follow seqerakit documentation)
# Typically involves setting environment variables:
export TOWER_ACCESS_TOKEN="your_tower_token"
export TOWER_API_ENDPOINT="https://tower.sagebionetworks.org/api"
```

## 🚀 Usage

### Step 1: Generate Samplesheet

Run the shortlist script to identify files needing processing:

```bash
# Basic usage - uses all defaults
python shortlist.py

# With custom options
python shortlist.py \
  --sample-size 20 \
  --cloud-provider s3 \
  --output-dir my_samplesheets \
  --debug
```

#### Shortlist Options:
```bash
python shortlist.py --help
```

Key options:
- `--assets-uri`: S3 URI for imaging assets JSON (default: s3://htan-assets/final-output/htan-imaging-assets-latest.json)
- `--aws-profile`: AWS profile name (default: htan-dev)
- `--bigquery-project`: BigQuery project ID (default: htan-dcc)
- `--output-dir`: Output directory for samplesheets (default: samplesheet/)
- `--sample-size`: Size of sample samplesheet (default: 10)
- `--cloud-provider`: Filter by cloud provider: s3, gs, or all (default: all)
- `--save-assets`: Save raw assets JSON locally
- `--debug`: Enable debug logging

### Step 2: Launch Pipeline

Launch the nf-artist pipeline with the generated samplesheet:

```bash
# Basic usage - launches with sample samplesheet
python launch.py

# With custom options
python launch.py \
  --samplesheet samplesheet/artist-samplesheet.csv \
  --workspace "your_workspace_id" \
  --keep-params \
  --debug
```

#### Launch Options:
```bash
python launch.py --help
```

Key options:
- `--samplesheet`: Path to samplesheet CSV (default: samplesheet/artist-samplesheet-sample10.csv)
- `--workspace`: Seqera Platform workspace ID (default: 253119656982040)
- `--compute-env`: Compute environment ID (default: 3k6bQPIqpII2nGlDMbc9Mv)
- `--output-bucket`: S3 bucket for outputs (default: s3://htan-project-tower-bucket/outputs)
- `--pipeline-url`: GitHub URL of nf-artist pipeline
- `--revision`: Pipeline revision/branch (default: main)
- `--keep-params`: Keep parameter file after launch (default: remove it)
- `--debug`: Enable debug logging

## 📁 Output Files

### Generated by shortlist.py:
- `samplesheet/artist-samplesheet.csv` - Full samplesheet with all files needing processing
- `samplesheet/artist-samplesheet-sample10.csv` - Sample subset for testing
- `htan-imaging-assets-latest.json` - Raw assets data (if `--save-assets` used)

### Generated by launch.py:
- `params/params_[ID].yaml` - Pipeline parameters file (auto-cleaned unless `--keep-params`)

## 🔧 Configuration

### Environment Variables

You can set these environment variables in a `.env` file:

```bash
# AWS Configuration
AWS_PROFILE=htan-dev

# Google Cloud Configuration  
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
GOOGLE_CLOUD_PROJECT=htan-dcc

# Seqera Platform Configuration
TOWER_ACCESS_TOKEN=your_tower_token
TOWER_API_ENDPOINT=https://tower.sagebionetworks.org/api
```

### Default Values

Key defaults that can be customized:
- **Workspace**: `253119656982040` (htan-project)
- **Compute Environment**: `3k6bQPIqpII2nGlDMbc9Mv` (htan-project-spot-v12)
- **Pipeline**: `https://github.com/Sage-Bionetworks-Workflows/nf-artist`
- **Output Bucket**: `s3://htan-project-tower-bucket/outputs`

## 📊 Workflow Logic

### Shortlisting Process:

1. **Fetch Assets Data**: Downloads latest imaging assets from S3
2. **Extract Existing**: Identifies files that already have thumbnails/Minerva
3. **Query BigQuery**: Gets released imaging files from HTAN DCC
4. **Filter & Process**: Identifies files needing processing
5. **Generate Samplesheets**: Creates CSV files for pipeline input

### Pipeline Launch Process:

1. **Validate Samplesheet**: Checks file exists and has required columns
2. **Upload Dataset**: Uploads samplesheet to Seqera Platform
3. **Create Parameters**: Generates YAML parameters file
4. **Launch Pipeline**: Submits job to Seqera Platform
5. **Cleanup**: Removes temporary files (unless `--keep-params`)

## 🐛 Troubleshooting

### Common Issues:

#### BigQuery Access Denied
```bash
# Ensure you're authenticated and have the right project
gcloud auth list
gcloud config get-value project
```

#### AWS S3 Access Denied
```bash
# Check your AWS profile
aws sts get-caller-identity --profile htan-dev
```

#### Seqera Platform Authentication
```bash
# Verify your token is set
echo $TOWER_ACCESS_TOKEN
```

#### Python Dependencies
```bash
# Reinstall dependencies
uv pip install --force-reinstall -r requirments.txt
```

### Debug Mode

Use `--debug` flag for verbose logging:
```bash
python shortlist.py --debug
python launch.py --debug
```

## 📝 Development

### Adding Dependencies

1. Add package to `requirments.in`
2. Compile new requirements:
   ```bash
   uv pip compile requirments.in -o requirments.txt
   ```
3. Install updated requirements:
   ```bash
   uv pip install -r requirments.txt
   ```

### Code Style

Both scripts follow these best practices:
- Type hints for function parameters and returns
- Comprehensive error handling and logging
- Modular function design
- Command-line argument parsing
- Configurable defaults

## 📄 License

This project is part of the HTAN (Human Tumor Atlas Network) initiative.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📞 Support

For issues or questions:
- Create an issue in this repository
- Contact the HTAN development team
