import pandas as pd
import os
import sys
import hashlib
import getpass
from datetime import datetime

# Add the src directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.utils.validation import validate_data_content

class ServerDataExtractor:
    """
    Agent for extracting server data based on parameters.
    """
    def __init__(self, data_dir="src/data/input", output_data_dir="src/data/output"):
        """
        Initialize the server data extractor.
        
        Args:
            data_dir: Directory containing input data files
            output_data_dir: Directory for output files
        """
        self.data_dir = data_dir
        self.output_data_dir = output_data_dir
        self.extraction_file_path = os.path.join(data_dir, "extraction_parameters.xlsx")
        self.data_file_path = os.path.join(data_dir, "Control3_Environment_Segregation_MockData_v4.xlsx")
        self.output_file = None
        self.agent_name = "Server Data Extractor"
        self.extraction_params = None
        self.server_data = None
        self.filtered_server_data = None
        self.server_instance_mapping = None  # To store the server instance mapping
        self.system_names_list = []
        self.client_name = "Default"
        
        # Create output directory if it doesn't exist
        os.makedirs(output_data_dir, exist_ok=True)
    
    def run(self):
        """
        Run the extraction process.
        
        Returns:
            bool: True if extraction was successful, False otherwise
        """
        try:
            # Step 1: Load the data
            if not self._load_data():
                return False
                
            # Step 2: Validate the data
            if not self._validate_data():
                return False
                
            # Step 3: Extract client name
            self._extract_client_name()
                
            # Step 4: Filter server data based on system names
            if not self._filter_server_data():
                return False
                
            # Step 5: Prepare output file path
            self._prepare_output_path()
            
            return True
            
        except Exception as e:
            print(f"Error during extraction process: {str(e)}")
            return False
    
    def _load_data(self):
        """
        Load data from input files.
        
        Returns:
            bool: True if data loading was successful, False otherwise
        """
        try:
            # Check if files exist before trying to open them
            if not os.path.exists(self.extraction_file_path):
                print(f"Extraction parameters file not found: {self.extraction_file_path}")
                return False
                
            if not os.path.exists(self.data_file_path):
                print(f"Server data file not found: {self.data_file_path}")
                return False
            
            # Read extraction parameters from Sheet1
            self.extraction_params = pd.read_excel(self.extraction_file_path, sheet_name="Sheet1")
            print(f"Loaded extraction parameters with {len(self.extraction_params)} records")
            
            # Read server data from Server_Instance_Mapping sheet
            self.server_data = pd.read_excel(self.data_file_path, sheet_name="Server_Instance_Mapping")
            print(f"Loaded server data with {len(self.server_data)} records")
            
            # Also load the server instance mapping (same as server_data in this case)
            self.server_instance_mapping = self.server_data.copy()
            
            return True
                
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            return False
    
    def _validate_data(self):
        """
        Validate the loaded data.
        
        Returns:
            bool: True if data validation was successful, False otherwise
        """
        if self.extraction_params is None or self.server_data is None:
            print("No data to validate")
            return False
            
        # Basic validation - check if dataframes have data
        if self.extraction_params.empty:
            print("Extraction parameters dataframe is empty")
            return False
            
        if self.server_data.empty:
            print("Server data dataframe is empty")
            return False
            
        # Check for required columns in extraction_params
        if 'System Name' not in self.extraction_params.columns:
            print("Missing required column 'System Name' in extraction parameters")
            return False
            
        # Check for required columns in server_data
        required_columns = ['System Name', 'Environment Type', 'Server/Instance ID', 'Hostname']
        missing_columns = [col for col in required_columns if col not in self.server_data.columns]
        if missing_columns:
            print(f"Missing required columns in server data: {', '.join(missing_columns)}")
            return False
        
        return True
    
    def _extract_client_name(self):
        """
        Extract client name from extraction parameters.
        """
        if 'Client Name' in self.extraction_params.columns and not self.extraction_params['Client Name'].empty:
            # Use the first client name in the list
            self.client_name = str(self.extraction_params['Client Name'].iloc[0]).strip()
            # Remove any characters that are not suitable for filenames
            self.client_name = ''.join(c for c in self.client_name if c.isalnum() or c in [' ', '_', '-'])
            self.client_name = self.client_name.replace(' ', '_')
    
    def _filter_server_data(self):
        """
        Filter server data based on system names from extraction parameters.
        
        Returns:
            bool: True if filtering was successful, False otherwise
        """
        for _, row in self.extraction_params.iterrows():
            if 'System Name' in row and pd.notna(row['System Name']):
                if row['System Name'].strip().lower() == 'all':
                    # If "All" is specified, no filtering needed
                    self.filtered_server_data = self.server_data
                    self.system_names_list = ["All"]
                    break
                else:
                    # Parse comma-separated system names
                    system_names = [s.strip() for s in row['System Name'].split(',')]
                    self.system_names_list = system_names
                    
                    # Filter data based on system names
                    self.filtered_server_data = self.server_data[self.server_data['System Name'].isin(system_names)]
        
        if self.filtered_server_data is None or self.filtered_server_data.empty:
            print("No matching records found or no valid system names provided")
            return False
            
        return True
    
    def _prepare_output_path(self):
        """
        Prepare the output file path.
        """
        self.output_file = os.path.join(self.output_data_dir, f"{self.client_name}_Server_Data.xlsx")
    
    def save_verified_population_file(self):
        """
        Save the extracted data to an Excel file with metadata.
        
        Returns:
            str: Path to the saved file, or None if saving failed
        """
        if self.filtered_server_data is None or self.output_file is None:
            print("No data to save or output file not specified")
            return None
        
        try:
            # Create metadata
            metadata = self._create_metadata()
            
            # Create a writer object to write multiple sheets to the same Excel file
            with pd.ExcelWriter(self.output_file) as writer:
                # Write the extracted server data to the "Server_Instance_Mapping" sheet
                self.filtered_server_data.to_excel(writer, sheet_name="Server_Instance_Mapping", index=False)
                
                # Write the metadata to the "Metadata" sheet
                metadata.to_excel(writer, sheet_name="Metadata", index=False)
            
            print(f"Data saved to {self.output_file}")
            return self.output_file
            
        except Exception as e:
            print(f"Error saving data: {str(e)}")
            return None
    
    def _create_metadata(self):
        """
        Create metadata DataFrame with extraction information.
        
        Returns:
            DataFrame containing the metadata
        """
        # Calculate hash total
        hash_total = self._calculate_hash_total(self.filtered_server_data)
        
        # Create metadata entry
        metadata_entry = {
            'Extraction timestamp': [datetime.now()],
            'Extracted by user ID': [getpass.getuser()],
            'Agentic/Non-agentic process': ['Agentic'],
            'Agent name': [self.agent_name],
            'System name': [", ".join(self.system_names_list)],
            'Record count': [len(self.filtered_server_data)],
            'Hash total': [hash_total],
            'Parameter file used': [os.path.basename(self.extraction_file_path)]
        }
        
        return pd.DataFrame(metadata_entry)
    
    def _calculate_hash_total(self, dataframe):
        """
        Calculate a hash total for the dataframe to use as a checksum.
        
        Args:
            dataframe: The pandas DataFrame to calculate hash for
        
        Returns:
            String hash value
        """
        # Convert dataframe to string and calculate MD5 hash
        df_string = dataframe.to_string()
        return hashlib.md5(df_string.encode()).hexdigest()