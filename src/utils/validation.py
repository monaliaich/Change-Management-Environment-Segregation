import os
import pandas as pd

def validate_input_files(extraction_file_path, data_file_path):
    """
    Validate that input files exist and are in the correct format.
    
    Args:
        extraction_file_path: Path to the extraction parameters file
        data_file_path: Path to the environment data file
    
    Returns:
        Tuple containing (validation result, message)
    """
    # Check if files exist
    if not os.path.exists(extraction_file_path):
        return False, f"Extraction parameters file not found: {extraction_file_path}"
    
    if not os.path.exists(data_file_path):
        return False, f"Environment data file not found: {data_file_path}"
    
    # Check if files are Excel files
    if not extraction_file_path.endswith(('.xlsx', '.xls')):
        return False, f"Extraction parameters file is not an Excel file: {extraction_file_path}"
    
    if not data_file_path.endswith(('.xlsx', '.xls')):
        return False, f"Environment data file is not an Excel file: {data_file_path}"
    
    try:
        # Check if required sheets exist
        extraction_sheets = pd.ExcelFile(extraction_file_path).sheet_names
        if "Sheet1" not in extraction_sheets:
            return False, f"Required sheet 'Sheet1' not found in extraction parameters file"
        
        data_sheets = pd.ExcelFile(data_file_path).sheet_names
        if "Environment_Register" not in data_sheets:
            return False, f"Required sheet 'Environment_Register' not found in environment data file"
        
        return True, "Input files validated successfully"
    
    except Exception as e:
        return False, f"Error validating input files: {str(e)}"

def validate_data_content(extraction_params, env_data):
    """
    Validate that the data content has the required columns and format.
    
    Args:
        extraction_params: DataFrame containing extraction parameters
        env_data: DataFrame containing environment data
    
    Returns:
        Tuple containing (validation result, message)
    """
    # Check if required columns exist in extraction parameters
    if 'Client Name' not in extraction_params.columns or 'System Name' not in extraction_params.columns:
        return False, "Required columns 'Client Name' or 'System Name' not found in extraction parameters"
    
    # Check if required columns exist in environment data
    if 'Env-ID' not in env_data.columns or 'System Name' not in env_data.columns:
        return False, "Required columns 'Env-ID' or 'System Name' not found in Environment_Register"
    
    # Check for missing values in required columns
    if env_data['Env-ID'].isnull().any() or env_data['System Name'].isnull().any():
        # Filter out rows with missing values
        env_data = env_data.dropna(subset=['Env-ID', 'System Name'])
        if env_data.empty:
            return False, "All records have missing values in required columns 'Env-ID' or 'System Name'"
        return True, "Some records with missing values in required columns were filtered out"
    
    return True, "Data content validated successfully"