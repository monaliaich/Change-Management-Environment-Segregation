import pytest
import pandas as pd
import os
import sys
import json
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
from datetime import datetime

# Add the src directory to the Python path
sys.path.append(str(Path(__file__).parent.parent))

# Import the class to test
from src.agents.environment_deviation_analyzer import EnvironmentDeviationAnalyzer

@pytest.fixture
def mock_env_data():
    """Create sample environment data for testing"""
    return pd.DataFrame({
        'System Name': ['System1', 'System1', 'System1', 'System2', 'System2', 'System3'],
        'Environment Type': ['DEV', 'TEST', 'PROD', 'DEV', 'PROD', 'DEV']
    })

@pytest.fixture
def mock_analysis_results():
    """Create sample analysis results for testing"""
    return pd.DataFrame({
        'System_Name': ['System1', 'System2', 'System3'],
        'Environment_DTAP': ['OK', 'Deviation', 'Deviation'],
        'Reason': [
            'DEV, TEST, PROD environments are present',
            'No TEST environment available',
            'No TEST, PROD environments available'
        ]
    })

@pytest.fixture
def analyzer():
    """Create an instance of EnvironmentDeviationAnalyzer for testing"""
    with patch('src.agents.environment_deviation_analyzer.load_config') as mock_config:
        mock_config.return_value = {
            'azure_ai_foundry': {
                'endpoint': 'https://test-endpoint.com',
                'deployment_name': 'test-deployment'
            }
        }
        with patch('src.agents.environment_deviation_analyzer.AgentsClient'):
            with patch('src.agents.environment_deviation_analyzer.AzureCliCredential'):
                analyzer = EnvironmentDeviationAnalyzer(
                    input_dir="test_input",
                    output_dir="test_output"
                )
                return analyzer

class TestEnvironmentDeviationAnalyzer:
    
    def test_init(self, analyzer):
        """Test initialization of EnvironmentDeviationAnalyzer"""
        assert analyzer.name == "EnvironmentDeviationAnalyzer"
        assert analyzer.input_dir == "test_input"
        assert analyzer.output_dir == "test_output"
        assert analyzer.input_file is None
        assert analyzer.env_data is None
        assert analyzer.analysis_results is None
        assert analyzer.client_name is None
    
    @patch('os.path.exists')
    @patch('glob.glob')
    def test_find_latest_input_file(self, mock_glob, mock_exists, analyzer):
        """Test finding the latest input file"""
        mock_glob.return_value = ["test_input/Client1_Environment_Data.xlsx"]
        mock_exists.return_value = True
        
        result = analyzer.find_latest_input_file()
        
        assert result is True
        assert analyzer.input_file == "test_input/Client1_Environment_Data.xlsx"
        assert analyzer.client_name == "Client1"
    
    @patch('os.path.exists')
    @patch('pandas.ExcelFile')
    def test_load_data_success(self, mock_excel_file, mock_exists, analyzer, mock_env_data):
        """Test loading data successfully"""
        # Setup mocks
        mock_exists.return_value = True
        mock_excel = MagicMock()
        mock_excel.sheet_names = ["Environment_Register"]
        mock_excel_file.return_value = mock_excel
        
        with patch('pandas.read_excel', return_value=mock_env_data):
            analyzer.input_file = "test_input/Client1_Environment_Data.xlsx"
            result = analyzer.load_data()
            
            assert result is True
            assert analyzer.env_data is not None
            assert len(analyzer.env_data) == 6
    
    @patch('os.path.exists')
    def test_load_data_file_not_found(self, mock_exists, analyzer):
        """Test loading data when file doesn't exist"""
        mock_exists.return_value = False
        analyzer.input_file = "test_input/NonExistent.xlsx"
        
        result = analyzer.load_data()
        
        assert result is False
    
    def test_prepare_data_for_analysis(self, analyzer, mock_env_data):
        """Test preparing data for analysis"""
        analyzer.env_data = mock_env_data
        
        result = analyzer._prepare_data_for_analysis()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3  # 3 unique systems
        assert 'System Name' in result.columns
        assert 'Environments' in result.columns
    
    @patch('asyncio.run')
    def test_analyze_environment_deviations(self, mock_asyncio_run, analyzer, mock_env_data, mock_analysis_results):
        """Test analyzing environment deviations"""
        analyzer.env_data = mock_env_data
        mock_asyncio_run.return_value = [mock_analysis_results.to_dict('records')]
        
        result = analyzer.analyze_environment_deviations()
        
        assert result is True
        assert analyzer.analysis_results is not None
        assert len(analyzer.analysis_results) == 3
    
    @patch('pandas.ExcelWriter')
    def test_save_analysis_results(self, mock_excel_writer, analyzer, mock_env_data, mock_analysis_results):
        """Test saving analysis results"""
        analyzer.env_data = mock_env_data
        analyzer.analysis_results = mock_analysis_results
        analyzer.input_file = "test_input/Client1_Environment_Data.xlsx"
        
        result = analyzer.save_analysis_results()
        
        assert result is not None
        assert "Client1_Environment_Deviation_Analysis.xlsx" in result
    
    def test_extract_json_from_text(self, analyzer):
        """Test extracting JSON from text"""
        # Test with JSON in code block
        text1 = "```json\n[{\"System_Name\": \"System1\", \"Environment_DTAP\": \"OK\"}]\n```"
        result1 = analyzer._extract_json_from_text(text1)
        assert isinstance(result1, list)
        assert len(result1) == 1
        assert result1[0]["System_Name"] == "System1"
        
        # Test with plain JSON array
        text2 = "[{\"System_Name\": \"System1\", \"Environment_DTAP\": \"OK\"}]"
        result2 = analyzer._extract_json_from_text(text2)
        assert isinstance(result2, list)
        assert len(result2) == 1
        
        # Test with invalid JSON
        text3 = "This is not JSON"
        result3 = analyzer._extract_json_from_text(text3)
        assert result3 == []
    
    @patch('src.agents.environment_deviation_analyzer.EnvironmentDeviationAnalyzer.load_data')
    @patch('src.agents.environment_deviation_analyzer.EnvironmentDeviationAnalyzer.analyze_environment_deviations')
    @patch('src.agents.environment_deviation_analyzer.EnvironmentDeviationAnalyzer.save_analysis_results')
    def test_run_success(self, mock_save, mock_analyze, mock_load, analyzer):
        """Test running the complete workflow successfully"""
        mock_load.return_value = True
        mock_analyze.return_value = True
        mock_save.return_value = "test_output/Client1_Environment_Deviation_Analysis.xlsx"
        
        result = analyzer.run()
        
        assert result is True
        mock_load.assert_called_once()
        mock_analyze.assert_called_once()
        mock_save.assert_called_once()
    
    @patch('src.agents.environment_deviation_analyzer.EnvironmentDeviationAnalyzer.load_data')
    def test_run_load_failure(self, mock_load, analyzer):
        """Test running the workflow when loading data fails"""
        mock_load.return_value = False
        
        result = analyzer.run()
        
        assert result is False
        mock_load.assert_called_once()