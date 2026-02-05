# Environment Segregation Analysis Tool

This tool analyzes system environments to ensure proper segregation between DEV, TEST, and PROD environments across various IT systems. It extracts data from input files and performs deviation analysis to identify systems that don't have all required environments.

## Table of Contents

1. [Overview](#overview)
2. [Features](#features)
3. [Installation](#installation)
4. [Project Structure](#project-structure)
5. [Configuration](#configuration)
6. [Usage](#usage)
7. [Process Descriptions](#process-descriptions)
8. [Output](#output)
9. [Troubleshooting](#troubleshooting)

## Overview

The Environment Segregation Analysis Tool is designed to help organizations ensure proper environment segregation in their IT systems. It analyzes various aspects of system environments including:

- Environment registers
- Database configurations
- Server instances
- URL endpoints
- Cloud resources

For each system, the tool checks if all three required environments (DEV, TEST, PROD) are present and reports any deviations.

## Features

- Extract environment data from input Excel files
- Analyze environment segregation across multiple dimensions
- Generate detailed reports of deviations
- Support for both one-time execution and scheduled runs
- AI-powered analysis using Azure AI Foundry

## Installation

### Prerequisites

- Python 3.8 or higher
- Azure CLI (for authentication with Azure AI Foundry)
- Azure AI Foundry access

### Clone the Repository

```bash
git clone https://github.com/monaliaich/Change-Management-Environment-Segregation.git
cd Change-Management-Environment-Segregation
```

### Set Up Virtual Environment

```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Set Up Environment Variables

Create a `.env` file in the project root with the following variables:

```
PROJECT_ENDPOINT=your_azure_ai_foundry_endpoint
AGENT_MODEL_DEPLOYMENT_NAME=your_model_deployment_name
```

## Project Structure

```
environment-segregation-analysis/
├── src/
│   ├── agents/
│   │   ├── extraction_logic.py                  # Environment register extractor
│   │   ├── environment_deviation_analyzer.py     # Environment deviation analyzer
│   │   ├── database_extraction_logic.py         # Database extractor
│   │   ├── database_deviation_analyzer.py       # Database deviation analyzer
│   │   ├── server_extraction_logic.py           # Server extractor
│   │   ├── server_deviation_analyzer.py         # Server deviation analyzer
│   │   ├── url_extraction_logic.py              # URL endpoint extractor
│   │   ├── url_endpoint_deviation_analyzer.py   # URL endpoint deviation analyzer
│   │   ├── cloud_extraction_logic.py            # Cloud resource extractor
│   │   └── cloud_resource_deviation_analyzer.py # Cloud resource deviation analyzer
│   ├── data/
│   │   ├── input/                               # Input data files
│   │   │   ├── extraction_parameters.xlsx       # Parameters for extraction
│   │   │   └── Control3_Environment_Segregation_MockData_v4.xlsx  # Mock data
│   │   └── output/                              # Output files
│   ├── utils/
│   │   ├── config.py                            # Configuration utilities
│   │   ├── validation.py                        # Data validation utilities
│   │   └── scheduler.py                         # Scheduler utilities
│   ├── workflow_manager.py                      # Manages extraction and analysis workflows
│   └── scheduler_manager.py                     # Manages scheduled executions
├── main.py                                      # Main entry point
├── requirements.txt                             # Python dependencies
└── .env                                         # Environment variables
```

## Configuration

### Input Data

1. Place your input data file in the `src/data/input` directory
2. Create an extraction parameters file in the same directory with the following structure:
   - Sheet1: Contains columns for "Client Name" and "System Name"

### Azure AI Foundry Configuration

1. Ensure you have access to Azure AI Foundry
2. Set up the required environment variables in the `.env` file

## Usage

### Running the Tool

You can run the tool in two modes:

1. **Single Execution Mode**:

```bash
# Run a specific process
python src/main.py --process env  # For environment register analysis
python src/main.py --process db   # For database analysis
python src/main.py --process server  # For server analysis
python src/main.py --process url  # For URL endpoint analysis
python src/main.py --process cloud  # For cloud resource analysis

# Run all processes
python src/main.py --process all
```

2. **Scheduled Mode**:

```bash
# Schedule a specific process to run every 5 minutes for 1 hour
python src/main.py --mode schedule --process env --interval 5 --duration 60
python src/main.py --mode schedule --process db --interval 1 --duration 10

# Schedule all processes to run indefinitely
python src/main.py --mode schedule --process all --duration 0
```

### Command Line Arguments

- `--mode`: Operation mode (`run` or `schedule`)
- `--process`: Process to run (`env`, `db`, `server`, `url`, `cloud`, or `all`)
- `--interval`: Interval in minutes for scheduled execution (default: 5)
- `--duration`: Duration in minutes for scheduled execution (default: 60, 0 for indefinite)
- `--data-dir`: Directory containing input files (default: src/data/input)
- `--output-dir`: Directory for output files (default: src/data/output)

## Process Descriptions

### Environment Register Analysis

Checks if each system has all three required environments (DEV, TEST, PROD) in the environment register.

### Database Analysis

Checks if each system has database configurations for all three required environments.

### Server Analysis

Checks if each system has server instances for all three required environments.

### URL Endpoint Analysis

Checks if each system has URL endpoints defined for all three required environments.

### Cloud Resource Analysis

Checks if each system has cloud resources provisioned for all three required environments.

## Output

For each process, the tool generates two Excel files:

1. **Extracted Data File**: Contains the extracted data for the specific process
   - Example: `ClientName_Environment_Data.xlsx`

2. **Analysis Results File**: Contains the analysis results showing deviations
   - Example: `ClientName_Environment_Data_Deviation_Analysis.xlsx`

Each analysis file contains:
- The original extracted data
- The deviation analysis results
- Metadata about the analysis

## Troubleshooting

### Common Issues

1. **Missing Environment Variables**:
   - Ensure all required environment variables are set in the `.env` file

2. **Authentication Issues**:
   - Run `az login` to authenticate with Azure CLI
   - Verify you have access to the Azure AI Foundry resources

3. **Input File Issues**:
   - Verify the input files exist in the correct location
   - Ensure the input files have the required sheets and columns

4. **No Results Generated**:
   - Check the logs for error messages
   - Verify that the extraction parameters are correctly set

### Logging

The tool logs information to both the console and a log file (`environment_data_extraction.log`). Check this file for detailed error messages and debugging information.

---

For additional support or to report issues, please contact the development team or create an issue in the GitHub repository.