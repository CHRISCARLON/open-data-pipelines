# Open Street Works 🚙

Data pipelines for processing and analysing UK street works.

## Overview

This project automates the extraction, loading, and transformation (ELT) of data from:

- Street Manager (Street Works for England only)
- Ordnance Survey Linked Identifiers
- Ordnance Survey Open USRNs
- Geoplace SWA Codes
- Scottish Roadworks Register (SRWR) - **TBC**

## Features

- **Process Raw Data**: Process raw data from Street Manager and other sources into a structured format
- **Load into MotherDuck**: Load the processed data into MotherDuck
- **DBT Analysis**: Run DBT models for transforming raw data into actionable insights - for example an england wide street work impact scores.

Check out [Word on the Street](https://word-on-the-street.evidence.app) for an example of a BI product that is based on this data pipeline.

## Components

- **Data Sources**: Configurable interfaces for different data providers
- **Data Processors**: Specialised handlers for each data source format
- **Database Layer**: Abstraction over MotherDuck/DuckDB connections and operations
- **Analysis**: DBT models for creating analytical aggregations
- **Infrastructure**: Terraform configurations for cloud deployment

## Getting Started

### Prerequisites

- Python 3.11+
- Poetry for dependency management
- AWS credentials (if using cloud deployment)
- MotherDuck token

FYI - This can be run:

- Locally with a Python venv
- Locally with Docker
- In the cloud with AWS Fargate (see terraform/main.tf)

### Installation

```bash
# Clone the repository
git clone https://github.com/your-username/open-street-works-data-pipeline.git
cd open-street-works-data-pipeline

# Install dependencies using Poetry
poetry install --no-root
```

### Configuration

Create a `.env` file with your configuration:

```zsh
# MotherDuck credentials
MOTHERDUCK_TOKEN=your_token
MOTHERDB=your_database

# AWS deployment (if using)
REGION=your_aws_region
ACCOUNT_ID=your_aws_account_id
REPO_NAME=your_ecr_repo_name
```

### Running the Pipeline

```bash
poetry run python -m src.main
```

## Deployment

If deploying to AWS Fargate, the project includes a Makefile to simplify Docker image building and AWS deployment:

```bash
# Build and push Docker image to ECR
make docker-all

# Apply Terraform configuration
cd terraform
terraform init
terraform apply
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
