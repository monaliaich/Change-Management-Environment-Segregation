import os
import sys
from dotenv import load_dotenv
from pathlib import Path

def load_config():
    """
    Load configuration from .env file
    
    Returns:
        dict: Configuration dictionary
    """
    # Find the src directory
    current_file = Path(__file__)
    src_dir = current_file.parent.parent
    
    # Load .env file from src directory
    env_path = src_dir / '.env'
    load_dotenv(dotenv_path=env_path)
    
    # Create configuration dictionary
    config = {
        'azure_ai_foundry': {
            'api_key': os.getenv('AZURE_API_KEY'),
            'endpoint': os.getenv('AZURE_ENDPOINT'),
            'deployment_name': os.getenv('AGENT_MODEL_DEPLOYMENT_NAME'),
            #'api_version': os.getenv('AZURE_AI_FOUNDRY_API_VERSION')
        }
    }
    
    # Validate required configuration
    if not all([
        config['azure_ai_foundry']['api_key'],
        config['azure_ai_foundry']['endpoint'],
        config['azure_ai_foundry']['deployment_name']
    ]):
        print("Error: Missing required Azure AI Foundry configuration in .env file")
        return None
    
    return config