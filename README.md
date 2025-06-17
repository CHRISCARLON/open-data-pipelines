# Open Infrastructure 🚙

Data pipelines for analysts working with UK infrastructure data.

## Overview

This project enables infrastructure analysts to perform analysis on UK infrastructure data by automating the extraction, loading, and transformation (ELT) of data from:

- Street Manager (Street Works for England only)
- Ordnance Survey Linked Identifiers
- Ordnance Survey Open USRNs
- Geoplace SWA Codes
- Scottish Roadworks Register (SRWR) **TBC**
- Dft Road Statistics **TBC**
- BDUK Premises Data
- Utility Company Open Data **TBC**

## Components

- **Data Sources**: Configurable interfaces for infrastructure data providers
- **Data Processors**: Specialized handlers for infrastructure data formats
- **Database Layer**: Abstraction over MotherDuck/DuckDB connections for analytical queries
- **Analysis**: DBT models for infrastructure analysts to derive insights
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
git clone https://github.com/your-username/open-infrastructure.git
cd open-infrastructure

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
