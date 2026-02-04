import pytest
import pandas as pd
import os
import sys
from unittest.mock import patch, MagicMock
from pathlib import Path
from datetime import datetime

# Add the src directory to the Python path
sys.path.append(str(Path(__file__).parent.parent))

# Import the class to test
from agents.extraction_logic import EnvironmentDataExtractor

@pytest.fixture
def mock_extraction_params():
    """Create sample extraction parameters for testing"""
    return pd.DataFrame({
        'Client Name': ['Test Client'],
        'System Name': ['System1,System2']
    })

@pytest.fixture
def mock_env_data_full():
    """Create full sample environment data for testing"""
    return pd.DataFrame({
        'System Name': ['System1', 'System1', 'System2', 'System2', 'System3'],
        'Environment Type': ['DEV', 'PROD', 'TEST', 'PROD', 'DEV'],
        'Additional Column': ['A', 'B', 'C', 'D', 'E']
    })

@pytest.fixture
def extractor():
    """Create an instance of EnvironmentDataExtractor for testing"""
    return EnvironmentDataExtractor(
        data_dir="test_data_dir",
        output_data_dir="test_output_dir"
    )

class TestEnvironmentDataExtractor:
    
    def test_init(self, extractor):
        """Test initialization of EnvironmentDataExtractor"""
        assert extractor.data_dir == "test_data_dir"
        assert extractor.output_data_dir == "test_output_dir"
        assert extractor.extraction_file_path == os.path.join("test_data_dir", "extraction_parameters.xlsx")
        assert extractor.data_file_path == "test_data_dir/Control3_Environment_Segregation_MockData_v4.xlsx"
        assert extractor.agent_name == "Environment Data Extractor"
    
    @patch('os.path.exists')
    @patch('pandas.read_excel')
    def test_load_data_success(self, mock_read_excel, mock_exists, extractor, mock_extraction_params, mock_env_data_full):
        """Test loading data successfully"""
        mock_exists.return_value = True
        mock_read_excel.side_effect = [mock_extraction_params, mock_env_data_full]
        
        result = extractor._load_data()
        
        assert result is True
        assert extractor.extraction_params is not None
        assert extractor.env_data is not None
    
    @patch('os.path.exists')
    def test_load_data_file_not_found(self, mock_exists, extractor):
        """Test loading data when file doesn't exist"""
        mock_exists.return_value = False
        
        result = extractor._load_data()
        
        assert result is False
    
    @patch('agents.extraction_logic.validate_data_content')
    def test_validate_data_success(self, mock_validate, extractor, mock_extraction_params, mock_env_data_full):
        """Test validating data successfully"""
        extractor.extraction_params = mock_extraction_params
        extractor.env_data = mock_env_data_full
        mock_validate.return_value = (True, "")
        
        result = extractor._validate_data()
        
        assert result is True
        mock_validate.assert_called_once()
    
    @patch('agents.extraction_logic.validate_data_content')
    def test_validate_data_failure(self, mock_validate, extractor, mock_extraction_params, mock_env_data_full):
        """Test validating data with failure"""
        extractor.extraction_params = mock_extraction_params
        extractor.env_data = mock_env_data_full
        mock_validate.return_value = (False, "Validation error")
        
        result = extractor._validate_data()
        
        assert result is False
        mock_validate.assert_called_once()
    
    def test_extract_client_name(self, extractor, mock_extraction_params):
        """Test extracting client name"""
        extractor.extraction_params = mock_extraction_params
        
        extractor._extract_client_name()
        
        assert extractor.client_name == "Test_Client"
    
    def test_filter_environment_data_specific_systems(self, extractor, mock_extraction_params, mock_env_data_full):
        """Test filtering environment data for specific systems"""
        extractor.extraction_params = mock_extraction_params
        extractor.env_data = mock_env_data_full
        
        result = extractor._filter_environment_data()
        
        assert result is True
        assert extractor.filtered_env_data is not None
        assert len(extractor.filtered_env_data) == 4  # Only System1 and System2 rows
        assert set(extractor.system_names_list) == {'System1', 'System2'}
    
    def test_filter_environment_data_all_systems(self, extractor, mock_env_data_full):
        """Test filtering environment data for all systems"""
        extractor.extraction_params = pd.DataFrame({
            'Client Name': ['Test Client'],
            'System Name': ['all']
        })
        extractor.env_data = mock_env_data_full
        
        result = extractor._filter_environment_data()
        
        assert result is True
        assert extractor.filtered_env_data is not None
        assert len(extractor.filtered_env_data) == 5  # All rows
        assert extractor.system_names_list == ["All"]
    
    def test_prepare_output_path(self, extractor):
        """Test preparing output path"""
        extractor.client_name = "Test_Client"
        
        extractor._prepare_output_path()
        
        assert extractor.output_file == os.path.join("test_output_dir", "Test_Client_Environment_Data.xlsx")
    
    @patch('pandas.ExcelWriter')
    @patch('agents.extraction_logic.EnvironmentDataExtractor._create_metadata')
    def test_save_verified_population_file(self, mock_create_metadata, mock_excel_writer, extractor, mock_env_data_full):
        """Test saving verified population file"""
        extractor.filtered_env_data = mock_env_data_full
        extractor.output_file = "test_output_dir/Test_Client_Environment_Data.xlsx"
        mock_create_metadata.return_value = pd.DataFrame({
            'Extraction timestamp': [datetime.now()],
            'Record count': [5]
        })
        
        # Mock the context manager behavior
        mock_writer_instance = MagicMock()
        mock_excel_writer.return_value.__enter__.return_value = mock_writer_instance
        
        result = extractor.save_verified_population_file()
        
        assert result == "test_output_dir/Test_Client_Environment_Data.xlsx"
        mock_create_metadata.assert_called_once()
    
    def test_calculate_hash_total(self, extractor, mock_env_data_full):
        """Test calculating hash total"""
        result = extractor._calculate_hash_total(mock_env_data_full)
        
        assert isinstance(result, str)
        assert len(result) == 32  # MD5 hash is 32 characters
    
    @patch('agents.extraction_logic.EnvironmentDataExtractor._load_data')
    @patch('agents.extraction_logic.EnvironmentDataExtractor._validate_data')
    @patch('agents.extraction_logic.EnvironmentDataExtractor._extract_client_name')
    @patch('agents.extraction_logic.EnvironmentDataExtractor._filter_environment_data')
    @patch('agents.extraction_logic.EnvironmentDataExtractor._prepare_output_path')
    def test_run_success(self, mock_prepare_path, mock_filter, mock_extract_name, mock_validate, mock_load, extractor):
        """Test running the complete workflow successfully"""
        mock_load.return_value = True
        mock_validate.return_value = True
        mock_filter.return_value = True
        
        result = extractor.run()
        
        assert result is True
        mock_load.assert_called_once()
        mock_validate.assert_called_once()
        mock_extract_name.assert_called_once()
        mock_filter.assert_called_once()
        mock_prepare_path.assert_called_once()
    
    @patch('agents.extraction_logic.EnvironmentDataExtractor._load_data')
    def test_run_load_failure(self, mock_load, extractor):
        """Test running the workflow when loading data fails"""
        mock_load.return_value = False
        
        result = extractor.run()
        
        assert result is False
        mock_load.assert_called_once()
    
    @patch('agents.extraction_logic.EnvironmentDataExtractor._load_data')
    @patch('agents.extraction_logic.EnvironmentDataExtractor._validate_data')
    def test_run_validate_failure(self, mock_validate, mock_load, extractor):
        """Test running the workflow when validation fails"""
        mock_load.return_value = True
        mock_validate.return_value = False
        
        result = extractor.run()
        
        assert result is False
        mock_load.assert_called_once()
        mock_validate.assert_called_once()
    
    @patch('agents.extraction_logic.EnvironmentDataExtractor._load_data')
    @patch('agents.extraction_logic.EnvironmentDataExtractor._validate_data')
    @patch('agents.extraction_logic.EnvironmentDataExtractor._extract_client_name')
    @patch('agents.extraction_logic.EnvironmentDataExtractor._filter_environment_data')
    def test_run_filter_failure(self, mock_filter, mock_extract_name, mock_validate, mock_load, extractor):
        """Test running the workflow when filtering fails"""
        mock_load.return_value = True
        mock_validate.return_value = True
        mock_filter.return_value = False
        
        result = extractor.run()
        
        assert result is False
        mock_load.assert_called_once()
        mock_validate.assert_called_once()
        mock_extract_name.assert_called_once()
        mock_filter.assert_called_once()



    @patch('os.path.exists')
    @patch('glob.glob')
    @patch('os.path.isfile')  # Add this if needed
    def test_find_latest_input_file(self, mock_glob, mock_exists, mock_isfile, analyzer):
        """Test finding the latest input file"""
        mock_glob.return_value = ["test_input/Client1_Environment_Data.xlsx"]
        mock_exists.return_value = True
        if 'mock_isfile' in locals():
            mock_isfile.return_value = True
        
        # If the method uses os.path.getmtime, mock that too
        with patch('os.path.getmtime', return_value=1234567890):
            result = analyzer.find_latest_input_file()
        
        assert result is True
        # Also check that the input_file attribute was set correctly
        assert analyzer.input_file == "test_input/Client1_Environment_Data.xlsx"     


    @patch('pandas.ExcelWriter')
    def test_save_analysis_results(self, mock_excel_writer, analyzer, mock_env_data, mock_analysis_results):
        """Test saving analysis results"""
        analyzer.env_data = mock_env_data
        analyzer.analysis_results = mock_analysis_results
        analyzer.input_file = "test_input/Client1_Environment_Data.xlsx"
        
        # Mock the context manager behavior
        mock_writer_instance = MagicMock()
        mock_excel_writer.return_value.__enter__.return_value = mock_writer_instance
        
        # If the method uses os.path.dirname or os.makedirs, mock those too
        with patch('os.path.dirname', return_value="test_output"):
            with patch('os.makedirs', return_value=None):
                result = analyzer.save_analysis_results()
        
        assert result is not None
        # You might want to check the specific return value if known  
         
    @pytest.mark.asyncio
    async def test_async_method(self, analyzer):
        result = await analyzer._process_batches_async()
        assert result is not None      