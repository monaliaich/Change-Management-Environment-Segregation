import os
import sys
import logging
import pandas as pd
import json
import re
import time
import traceback
import asyncio
import glob
import getpass
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from azure.ai.agents import AgentsClient
from azure.identity import AzureCliCredential
from azure.core.pipeline.transport import RequestsTransport

# Add the src directory to the Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.config import load_config

class CloudResourceDeviationAnalyzer:
    """
    Agent for analyzing cloud resource deviations in the extracted data.
    This agent checks if each system has proper cloud resource segregation.
    """
    def __init__(self, input_dir="src/data/output", output_dir="src/data/output"):
        """
        Initialize the cloud resource deviation analyzer.
        
        Args:
            input_dir: Directory containing input files (output from extraction process)
            output_dir: Directory for output files
        """
        self.name = "CloudResourceDeviationAnalyzer"
        self.logger = logging.getLogger(self.name)
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.input_file = None
        self.cloud_data = None
        self.cloud_resource_inventory = None
        self.analysis_results = None
        self.client_name = None
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Load environment variables from .env file
        from dotenv import load_dotenv
        if not load_dotenv():
            self.logger.warning("Could not load .env file, will use environment variables directly")
    
        # Validate required environment variables
        required = ["PROJECT_ENDPOINT", "AGENT_MODEL_DEPLOYMENT_NAME"]
        missing = [k for k in required if not os.environ.get(k)]
        if missing:
            self.logger.error("Missing env vars: %s", ", ".join(missing))
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

        # If validation passes, continue with setup
        self.PROJECT_ENDPOINT = os.environ.get("PROJECT_ENDPOINT")
        self.AGENT_MODEL_DEPLOYMENT_NAME = os.environ.get("AGENT_MODEL_DEPLOYMENT_NAME")
        
        # Initialize Azure AI Foundry client
        try:
            # Use RequestsTransport with verify=False to handle SSL issues if needed
            transport = RequestsTransport(connection_verify=False)
            
            self.client = AgentsClient(
                endpoint=self.PROJECT_ENDPOINT,
                credential=AzureCliCredential(),
                transport=transport
            )
            self.logger.info("Azure AI Foundry client initialized successfully")
        except Exception as e:
            self.logger.error(f"Error initializing Azure AI Foundry client: {str(e)}")
            self.logger.warning("Continuing without Azure AI Foundry integration")
            self.client = None

    def find_latest_input_file(self):
        """
        Find the latest Cloud_Resource_Data file in the input directory.
        
        Returns:
            bool: True if a file was found, False otherwise
        """
        try:
            # Look for files matching the pattern *Cloud_Resource_Data.xlsx
            pattern = os.path.join(self.input_dir, "*Cloud_Resource_Data.xlsx")
            matching_files = glob.glob(pattern)
            
            if not matching_files:
                self.logger.error(f"No Cloud_Resource_Data files found in {self.input_dir}")
                return False
            
            # Get the most recently modified file
            self.input_file = max(matching_files, key=os.path.getmtime)
            
            # Extract client name from the filename
            filename = os.path.basename(self.input_file)
            self.client_name = filename.split('_Cloud_Resource_Data')[0]
            
            self.logger.info(f"Found input file: {self.input_file}")
            self.logger.info(f"Client name: {self.client_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error finding input file: {str(e)}")
            return False         
        
    def load_data(self):
        """
        Load data from the input file with better error handling.
        
        Returns:
            bool: True if data loading was successful, False otherwise
        """
        try:
            # Find the latest input file if not already set
            if not self.input_file:
                if not self.find_latest_input_file():
                    return False
            
            if not os.path.exists(self.input_file):
                self.logger.error(f"Input file not found: {self.input_file}")
                return False
            
            # If the file exists but is corrupted, log the error
            try:
                # Try to read the Excel file
                excel_file = pd.ExcelFile(self.input_file)
                
                # Check if Cloud_Resource_Inventory sheet exists
                if "Cloud_Resource_Inventory" not in excel_file.sheet_names:
                    self.logger.error("Required sheet 'Cloud_Resource_Inventory' not found in input file")
                    return False
                
                # Load the Cloud_Resource_Inventory sheet
                self.cloud_data = pd.read_excel(excel_file, sheet_name="Cloud_Resource_Inventory")
                
            except Exception as excel_error:
                self.logger.error(f"Excel file appears to be corrupted: {str(excel_error)}")
                self.logger.info("Please regenerate the data by running: python main.py --process cloud")
                return False
            
            # Check if required columns exist
            required_columns = ["System Name", "Environment Type", "Subscription ID", "Resource Group Name"]
            missing_columns = [col for col in required_columns if col not in self.cloud_data.columns]
            if missing_columns:
                self.logger.error(f"Required columns {', '.join(missing_columns)} not found in Cloud_Resource_Inventory sheet")
                return False
            
            self.logger.info(f"Loaded {len(self.cloud_data)} records from {self.input_file}")
            return True
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            return False
        
    def analyze_cloud_deviations(self):
        """
        Analyze cloud resource deviations using Azure AI Foundry.
        
        Returns:
            bool: True if analysis was successful, False otherwise
        """
        try:
            if self.cloud_data is None or self.cloud_data.empty:
                self.logger.error("No data to analyze")
                return False
            
            # Prepare data for AI analysis
            data_for_analysis = self._prepare_data_for_analysis()
            
            # Process data in batches and send to AI for analysis
            all_results = self._process_batches_with_ai(data_for_analysis)
            
            if not all_results:
                self.logger.error("No results returned from AI analysis")
                return False
            
            # Convert results to DataFrame
            self.analysis_results = pd.DataFrame(all_results)
            
            self.logger.info(f"Analysis completed with {len(self.analysis_results)} results")
            return True
            
        except Exception as e:
            self.logger.error(f"Error analyzing cloud resource deviations: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return False   

    def _prepare_data_for_analysis(self):
        """
        Prepare data for cloud resource deviation analysis.
        
        Returns:
            DataFrame: Prepared data for analysis
        """
        # Group by System Name to get unique systems
        systems = self.cloud_data['System Name'].unique()
        
        # Create a list of dictionaries with system name and its environments
        data_for_analysis = []
        for system in systems:
            system_data = self.cloud_data[self.cloud_data['System Name'] == system]
            
            # Get list of unique environment types for this system
            environment_types = system_data['Environment Type'].unique().tolist()
            
            # Create explicit flags for environment presence
            has_dev = 'DEV' in environment_types
            has_test = 'TEST' in environment_types
            has_prod = 'PROD' in environment_types
            
            data_for_analysis.append({
                'System Name': system,
                'Environment Types': environment_types,
                'Has DEV': has_dev,
                'Has TEST': has_test,
                'Has PROD': has_prod
            })
        
        return pd.DataFrame(data_for_analysis)

    def _process_batches_with_ai(self, data):
        """
        Process data in batches and send to AI for analysis.
        
        Args:
            data: DataFrame containing data to analyze
            
        Returns:
            List of dictionaries containing analysis results
        """
        all_results = []
    
        # Process systems in smaller batches
        batch_size = 10  # Smaller batch size for better reliability
        self.logger.info(f"Processing {len(data)} systems in batches of {batch_size}")
        
        # Create batches
        batches = []
        for i in range(0, len(data), batch_size):
            batch = data.iloc[i:i+batch_size]
            batch_json = batch.to_json(orient='records')
            prompt = self._create_cloud_analysis_prompt(batch_json)
            batches.append((prompt, i, batch_size, len(data)))
        
        # Process batches asynchronously
        batch_results = asyncio.run(self._process_batches_async(batches))
        
        # Flatten results
        self.logger.info(f"Received {len(batch_results)} batch results")
        for i, results in enumerate(batch_results):
            self.logger.info(f"Batch {i+1} returned {len(results) if results else 0} results")
            if results and len(results) > 0:
                self.logger.info(f"Sample result keys: {list(results[0].keys())}")
        
        # Flatten results
        for results in batch_results:
            if results:
                all_results.extend(results)
        
        self.logger.info(f"Total results after processing: {len(all_results)}")
        return all_results    

    async def _process_batches_async(self, batches):
        """
        Process batches asynchronously.
        
        Args:
            batches: List of tuples containing (prompt, batch_index, batch_size, total_records)
            
        Returns:
            List of results from each batch
        """
        # Create tasks for each batch
        tasks = []
        for batch_data in batches:
            prompt, i, batch_size, total_records = batch_data
            task = asyncio.create_task(self._call_ai_for_analysis_async(prompt, i, batch_size, total_records))
            tasks.append(task)
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results, handling any exceptions
        processed_results = []
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Error processing batch {i}: {str(result)}")
                # Return empty list for this batch
                processed_results.append([])
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def _call_ai_for_analysis_async(self, prompt, batch_index, batch_size, total_records):
        """
        Call Azure AI Foundry for analysis asynchronously.
        
        Args:
            prompt: The prompt to send to the AI model
            batch_index: Index of the batch
            batch_size: Size of the batch
            total_records: Total number of records
            
        Returns:
            List of dictionaries containing analysis results
        """
        try:
            self.logger.info(f"Sending batch {batch_index//batch_size + 1}/{(total_records + batch_size - 1)//batch_size} to Azure AI Foundry")
            
            # Check if Azure AI client is available
            if not hasattr(self, 'client'):
                self.logger.error("Azure AI Foundry client not properly initialized")
                raise AttributeError("Azure AI Foundry client not properly initialized")
            
            # Create the system prompt
            system_prompt = """You are an expert IT cloud resource analyst specializing in compliance and cloud environment validation. 
            Your task is to analyze system cloud environments and identify deviations from the required environment setup."""
            
            # Use ThreadPoolExecutor to run synchronous API calls in a separate thread
            with ThreadPoolExecutor() as executor:
                future = executor.submit(
                    self._call_ai_with_thread_and_run,
                    system_prompt,
                    prompt,
                    max_retries=5
                )
                return await asyncio.wrap_future(future)
                
        except Exception as e:
            self.logger.error(f"Error getting AI analysis for batch {batch_index//batch_size + 1}: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return []
        
    def _create_cloud_analysis_prompt(self, data_json):
        """
        Create the prompt for cloud resource analysis.
        
        Args:
            data_json: JSON string containing cloud resource data
            
        Returns:
            String prompt for the AI model
        """
        return f"""
        You are an expert in IT cloud resource environment analysis. I need you to analyze the following system cloud environment data:
        
        {data_json}
        
        TASK:
        For each System Name, check ONLY if it has all three required environments: DEV, TEST, and PROD.
        
        INSTRUCTIONS:
        1. For each system, look at the boolean fields 'Has DEV', 'Has TEST', and 'Has PROD'.
        2. If all three are True, mark the system as "OK" with reason "DEV, TEST, PROD environments are present".
        3. If any are False, mark it as "Deviation" with the reason "No [ENVIRONMENT] environment available".
        4. If multiple environments are missing, list all missing environments in the reason.
        
        REQUIRED OUTPUT FORMAT:
        A JSON array containing one object for each System Name, with these exact fields:
        - System_Name: The system name
        - Cloud_Config: Either "Deviation" or "OK"
        - Reason: The reason for deviation, or "DEV, TEST, PROD environments are present" if OK
        
        EXAMPLES:
        1. If a system has all environments (Has DEV=True, Has TEST=True, Has PROD=True):
           {{"System_Name": "Workday Payroll", "Cloud_Config": "OK", "Reason": "DEV, TEST, PROD environments are present"}}
        
        2. If a system is missing TEST (Has DEV=True, Has TEST=False, Has PROD=True):
           {{"System_Name": "SAP FI", "Cloud_Config": "Deviation", "Reason": "No TEST environment available"}}
        
        3. If a system is missing multiple environments (Has DEV=False, Has TEST=False, Has PROD=True):
           {{"System_Name": "Oracle EBS AP", "Cloud_Config": "Deviation", "Reason": "No DEV and TEST environments available"}}
        
        CRITICAL:
        - Focus ONLY on the presence of environment types (DEV, TEST, PROD)
        - The 'Has DEV', 'Has TEST', and 'Has PROD' flags indicate if that environment type exists
        - Analyze EVERY System Name in the input data
        - Return ONLY the JSON array with no additional text
        """

    def _call_ai_with_thread_and_run(self, system_prompt, user_prompt, max_retries=3):
        """
        Call AI using the thread and run API pattern.
        
        Args:
            system_prompt: The system prompt
            user_prompt: The user prompt
            max_retries: Maximum number of retries
            
        Returns:
            List of dictionaries containing analysis results
        """
        retry_count = 0
        while retry_count < max_retries:
            try:
                # First create a thread
                thread_response = self.client.threads.create()
                thread_id = thread_response.id
                self.logger.info(f"Created thread with ID: {thread_id}")
                
                # Add system instructions as a user message
                system_instructions = f"You are an expert in IT cloud resource environment analysis. {system_prompt}"
                
                # Add user message with combined instructions and prompt
                self.client.messages.create(
                    thread_id=thread_id,
                    role="user",
                    content=f"{system_instructions}\n\n{user_prompt}"
                )
                
                # Try to get an agent ID using the helper method
                agent_id = self._get_agent_id()
                
                # Create a run
                if agent_id:
                    # Try with agent_id
                    self.logger.info(f"Creating run with agent_id: {agent_id}")
                    run_response = self.client.runs.create(
                        thread_id=thread_id,
                        agent_id=agent_id
                    )
                else:
                    # Try with model name directly
                    self.logger.info(f"Creating run with model: {self.AGENT_MODEL_DEPLOYMENT_NAME}")
                    run_response = self.client.runs.create(
                        thread_id=thread_id,
                        model=self.AGENT_MODEL_DEPLOYMENT_NAME
                    )
                
                run_id = run_response.id
                self.logger.info(f"Created run with ID: {run_id}")
                
                # Poll for completion
                result = self._poll_for_completion(thread_id, run_id)
                if result:
                    return result
                
                # If we got here, polling failed but didn't raise an exception
                self.logger.warning(f"Polling completed but no results returned. Retry {retry_count + 1}/{max_retries}")
                retry_count += 1
                
            except Exception as e:
                self.logger.error(f"Error using create_thread_and_run (attempt {retry_count + 1}/{max_retries}): {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    self.logger.info(f"Retrying in 2 seconds...")
                    time.sleep(2)  # Wait before retrying
        
        # If all retries fail, try the alternative methods as a last resort
        self.logger.error(f"Failed after {max_retries} attempts")
        return []
    
    def _get_agent_id(self):
        """
        Get an agent ID for the AI analysis.
        
        Returns:
            str: Agent ID or None if not available
        """
        try:
            # Try to list available agents
            agents_list = list(self.client.list_agents())
            self.logger.info(f"Available agents: {[a.id for a in agents_list]}")
            
            # If no agents are found, create one
            if not agents_list:
                self.logger.info("No agents found. Creating a new agent.")
                agent = self.client.create_agent(
                    name="Cloud Resource Deviation Analyzer",
                    description="Analyzes cloud resource environment deviations in IT systems",
                    model=self.AGENT_MODEL_DEPLOYMENT_NAME
                )
                return agent.id
            else:
                # Use the first available agent
                return agents_list[0].id
                
        except Exception as e:
            self.logger.error(f"Error getting agent ID: {str(e)}")
            return None
        
    def _poll_for_completion(self, thread_id, run_id, max_retries=30, retry_interval=2):
        """
        Poll for completion of the AI run.
    
        Args:
            thread_id: The thread ID
            run_id: The run ID
            max_retries: Maximum number of retries
            retry_interval: Interval between retries in seconds
            
        Returns:
            List of dictionaries containing analysis results
        """
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                run_status = self.client.runs.get(thread_id=thread_id, run_id=run_id)
                self.logger.info(f"Run status: {run_status.status}")
                
                if run_status.status == "completed":
                    # Get the messages from the thread
                    messages = self.client.messages.list(thread_id=thread_id)
                    
                    # Get the last assistant message
                    assistant_messages = [msg for msg in messages if msg.role == "assistant"]
                    
                    if assistant_messages:
                        # Extract and process the AI response
                        return self._extract_ai_response(assistant_messages[-1])
                    else:
                        self.logger.warning("No assistant messages found in the thread")
                        return []
                elif run_status.status in ["failed", "cancelled", "expired"]:
                    self.logger.error(f"Run failed with status: {run_status.status}")
                    return []
                
                # Wait before checking again
                time.sleep(retry_interval)
                retry_count += 1
            except Exception as e:
                self.logger.error(f"Error polling for completion: {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(retry_interval)
        
        self.logger.error(f"Run timed out after {max_retries * retry_interval} seconds")
        return []   

    def _extract_ai_response(self, message):
        """
        Extract the AI response from the message.
        
        Args:
            message: The message containing the AI response
            
        Returns:
            List of dictionaries containing analysis results
        """
        try:
            # Try to extract the text content
            ai_response_text = None
            
            # Check if content is available
            if hasattr(message, 'content'):
                content = message.content
                
                # If content is a list
                if isinstance(content, list):
                    for item in content:
                        if hasattr(item, 'text'):
                            text = item.text
                            if isinstance(text, str):
                                ai_response_text = text
                            elif hasattr(text, 'value'):
                                ai_response_text = text.value
                            else:
                                ai_response_text = str(text)
                
                # If content is a string
                elif isinstance(content, str):
                    ai_response_text = content
                # If content has a text attribute
                elif hasattr(content, 'text'):
                    ai_response_text = content.text
            
            # If we still don't have text, try other attributes
            if not ai_response_text and hasattr(message, 'text_messages') and message.text_messages:
                ai_response_text = message.text_messages[0]
            elif not ai_response_text and hasattr(message, 'text'):
                ai_response_text = message.text
            
            if not ai_response_text:
                self.logger.error("Could not extract text from message")
                return []
            
            # Use the improved JSON extraction method
            return self._extract_json_from_text(ai_response_text)
            
        except Exception as e:
            self.logger.error(f"Error extracting AI response: {str(e)}")
            return []
        
    def _extract_json_from_text(self, text):
        """
        Extract JSON from the text.
        
        Args:
            text: The text to extract JSON from
            
        Returns:
            List of dictionaries containing analysis results
        """
        try:
            # First, clean up the text to handle common formatting issues
            # Remove any markdown code block markers
            text = re.sub(r'```(?:json)?\s*|\s*```', '', text)
            
            # Look for JSON content between triple backticks
            json_match = re.search(r'```json\s*([\s\S]*?)\s*```', text)
            if json_match:
                json_content = json_match.group(1)
                self.logger.info("Found JSON array in code block")
            else:
                # Try to find any array in square brackets
                json_match = re.search(r'\[\s*{[\s\S]*?}\s*\]', text)
                if json_match:
                    json_content = json_match.group(0)
                    self.logger.info("Found JSON array")
                else:
                    # Try to find any object in curly braces
                    json_match = re.search(r'{[\s\S]*?}', text)
                    if json_match:
                        json_content = json_match.group(0)
                        self.logger.info("Found JSON object")
                    else:
                        # Try to parse the entire text as JSON
                        try:
                            json.loads(text)
                            json_content = text
                            self.logger.info("Using entire text as JSON")
                        except:
                            # Create a simple JSON structure with the text
                            self.logger.error("Could not find JSON content in the response")
                            return []
            
            # Parse the JSON content
            try:
                ai_response = json.loads(json_content)
                self.logger.info(f"Parsed JSON: {type(ai_response)}")
                
                # Process the response based on its structure
                if isinstance(ai_response, list):
                    return ai_response
                elif isinstance(ai_response, dict):
                    if 'results' in ai_response:
                        return ai_response['results']
                    else:
                        return [ai_response]
                else:
                    self.logger.warning(f"Unexpected JSON structure: {type(ai_response)}")
                    return []
                    
            except json.JSONDecodeError as e:
                self.logger.error(f"Error parsing JSON: {str(e)}")
                return []
        except Exception as e:
            self.logger.error(f"Error extracting JSON from text: {str(e)}")
            return []

    def save_analysis_results(self):
        """
        Save the analysis results to a new Excel file with input data, analysis results, and metadata.
        
        Returns:
            str: Path to the saved file, or None if saving failed
        """
        try:
            if self.analysis_results is None or self.analysis_results.empty:
                self.logger.error("No analysis results to save")
                return None
            
            if not self.input_file or not self.cloud_data.any().any():
                self.logger.error("No input data available")
                return None
            
            # Create output filename based on the input filename
            input_filename = os.path.basename(self.input_file)
            filename_without_ext = os.path.splitext(input_filename)[0]
            output_filename = f"{filename_without_ext}_Deviation_Analysis.xlsx"
            output_file_path = os.path.join(self.output_dir, output_filename)
            
            # Generate metadata
            metadata_df = self._create_metadata()
            
            # Create a writer object to write to a new Excel file - DON'T use with statement
            writer = pd.ExcelWriter(output_file_path, engine='openpyxl')
            
            # Write the input cloud resource data to the first sheet
            self.cloud_data.to_excel(writer, sheet_name="Cloud_Resource_Inventory", index=False)
            
            # Write the analysis results to the second sheet
            self.analysis_results.to_excel(writer, sheet_name="Cloud_Resource_Deviation_Analysis", index=False)
            
            # Write the metadata to the third sheet
            metadata_df.to_excel(writer, sheet_name="Metadata", index=False)
            
            # Explicitly close the writer
            writer.close()
            
            self.logger.info(f"Analysis results saved to {output_file_path}")
            return output_file_path
            
        except Exception as e:
            self.logger.error(f"Error saving analysis results: {str(e)}")
            return None

    def _create_metadata(self):
        """
        Create metadata DataFrame with analysis information.
        
        Returns:
            DataFrame containing the metadata
        """
        import getpass
        from datetime import datetime
        
        # Count records by status
        if self.analysis_results is not None and not self.analysis_results.empty:
            total_records = len(self.analysis_results)
            exception_records = len(self.analysis_results[self.analysis_results['Cloud_Config'] == 'Deviation'])
            ok_records = len(self.analysis_results[self.analysis_results['Cloud_Config'] == 'OK'])
            unknown_records = total_records - exception_records - ok_records
        else:
            total_records = 0
            exception_records = 0
            ok_records = 0
            unknown_records = 0
        
        # Create metadata entry
        metadata_entry = {
            'Key': [
                'user',
                'report_timestamp',
                'generated_by',
                'source_population_file',
                'total_records_analyzed',
                'exception_records',
                'ok_records',
                'unknown_records',
                'environment'
            ],
            'Value': [
                getpass.getuser(),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                self.name,
                os.path.basename(self.input_file) if self.input_file else 'Unknown',
                total_records,
                exception_records,
                ok_records,
                unknown_records,
                'Development'  # You might want to make this configurable
            ]
        }
        
        return pd.DataFrame(metadata_entry)

    def run(self):
        """
        Run the complete cloud resource deviation analysis workflow.
        
        Returns:
            bool: True if analysis was successful, False otherwise
        """
        try:
            self.logger.info("Starting Cloud Resource Deviation Analysis")
            
            # Step 1: Load data from the input file
            success = self.load_data()
            if not success:
                self.logger.error("Failed to load data")
                return False
            
            # Step 2: Analyze cloud resource deviations
            success = self.analyze_cloud_deviations()
            if not success:
                self.logger.error("Failed to analyze cloud resource deviations")
                return False
            
            # Step 3: Save analysis results
            output_file = self.save_analysis_results()
            if not output_file:
                self.logger.error("Failed to save analysis results")
                return False
            
            self.logger.info(f"Cloud Resource Deviation Analysis completed successfully. Results saved to {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error running Cloud Resource Deviation Analysis: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return False 
    