import logging
import os
import time
from datetime import datetime

class WorkflowManager:
    """
    Manager for orchestrating different extraction and analysis workflows.
    """
    def __init__(self, data_dir, output_dir):
        """
        Initialize the workflow manager.
        
        Args:
            data_dir: Directory containing input data files
            output_dir: Directory for output files
        """
        self.logger = logging.getLogger("WorkflowManager")
        self.data_dir = data_dir
        self.output_dir = output_dir
        
        # Ensure directories exist
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
    
    def run_environment_workflow(self):
        """
        Run the environment register extraction and analysis workflow.
        
        Returns:
            bool: True if workflow was successful, False otherwise
        """
        self.logger.info("Starting Environment Register Workflow")
        
        try:
            # Import the environment extractor
            from src.agents.extraction_logic import EnvironmentDataExtractor
            
            # Create and run the environment extractor
            env_extractor = EnvironmentDataExtractor(
                data_dir=self.data_dir,
                output_data_dir=self.output_dir
            )
            
            env_success = env_extractor.run()
            
            if env_success:
                env_output_file = env_extractor.save_verified_population_file()
                if env_output_file:
                    self.logger.info(f"Environment extraction completed successfully. Output file: {env_output_file}")
                    
                    # Run environment deviation analysis
                    env_analysis_success = self.run_environment_analysis()
                    if not env_analysis_success:
                        self.logger.error("Environment analysis failed")
                    
                    return True
                else:
                    self.logger.error("Failed to save environment output file")
                    return False
            else:
                self.logger.error("Environment extraction failed")
                return False
                
        except ImportError as e:
            self.logger.error(f"Error importing environment modules: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error in environment workflow: {str(e)}")
            return False
    
    def run_environment_analysis(self):
        """
        Run the environment deviation analysis.
        
        Returns:
            bool: True if analysis was successful, False otherwise
        """
        self.logger.info("Starting Environment Deviation Analysis")
        
        try:
            # Import the analyzer
            from src.agents.environment_deviation_analyzer import EnvironmentDeviationAnalyzer
            
            env_analyzer = EnvironmentDeviationAnalyzer(
                input_dir=self.output_dir,
                output_dir=self.output_dir
            )
            
            env_analysis_success = env_analyzer.run()
            
            if env_analysis_success:
                self.logger.info("Environment Deviation Analysis completed successfully")
                return True
            else:
                self.logger.error("Environment Deviation Analysis failed")
                return False
                
        except ImportError as e:
            self.logger.error(f"Error importing EnvironmentDeviationAnalyzer: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error running Environment Deviation Analysis: {str(e)}")
            return False
    
    def run_database_workflow(self):
        """
        Run the database extraction and analysis workflow.
        
        Returns:
            bool: True if workflow was successful, False otherwise
        """
        self.logger.info("Starting Database Workflow")
        
        try:
            # Import the database extractor
            from src.agents.database_extraction_logic import DatabaseDataExtractor
            
            # Create and run the database extractor
            db_extractor = DatabaseDataExtractor(
                data_dir=self.data_dir,
                output_data_dir=self.output_dir
            )
            
            db_success = db_extractor.run()
            
            if db_success:
                db_output_file = db_extractor.save_verified_population_file()
                if db_output_file:
                    self.logger.info(f"Database extraction completed successfully. Output file: {db_output_file}")
                    
                    # Run database instance analysis
                    db_analysis_success = self.run_database_analysis()
                    if not db_analysis_success:
                        self.logger.error("Database analysis failed")
                    
                    return True
                else:
                    self.logger.error("Failed to save database output file")
                    return False
            else:
                self.logger.error("Database extraction failed")
                return False
                
        except ImportError as e:
            self.logger.error(f"Error importing database modules: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error in database workflow: {str(e)}")
            return False
        
    def run_cloud_workflow(self):
        """
        Run the cloud resource extraction and analysis workflow.
        
        Returns:
            bool: True if workflow was successful, False otherwise
        """
        self.logger.info("Starting Cloud Resource Workflow")
        
        try:
            # Import the cloud resource extractor
            from src.agents.cloud_resource_extractor import CloudResourceExtractor
            
            # Create and run the cloud resource extractor
            cloud_extractor = CloudResourceExtractor(
                data_dir=self.data_dir,
                output_data_dir=self.output_dir
            )
            
            cloud_success = cloud_extractor.run()
            
            if cloud_success:
                cloud_output_file = cloud_extractor.save_verified_population_file()
                if cloud_output_file:
                    self.logger.info(f"Cloud resource extraction completed successfully. Output file: {cloud_output_file}")
                    
                    # Run cloud resource deviation analysis
                    cloud_analysis_success = self.run_cloud_analysis()
                    if not cloud_analysis_success:
                        self.logger.error("Cloud resource analysis failed")
                    
                    return True
                else:
                    self.logger.error("Failed to save cloud resource output file")
                    return False
            else:
                self.logger.error("Cloud resource extraction failed")
                return False
                
        except ImportError as e:
            self.logger.error(f"Error importing cloud resource modules: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error in cloud resource workflow: {str(e)}")
            return False    
    
    def run_database_analysis(self):
        """
        Run the database instance analysis.
        
        Returns:
            bool: True if analysis was successful, False otherwise
        """
        self.logger.info("Starting Database Instance Analysis")
        
        try:
            # Import the analyzer
            from src.agents.database_deviation_analyzer import DatabaseDeviationAnalyzer
            
            db_analyzer = DatabaseDeviationAnalyzer(
                input_dir=self.output_dir,
                output_dir=self.output_dir
            )
            
            db_analysis_success = db_analyzer.run()
            
            if db_analysis_success:
                self.logger.info("Database Instance Analysis completed successfully")
                return True
            else:
                self.logger.error("Database Instance Analysis failed")
                return False
                
        except ImportError as e:
            self.logger.error(f"Error importing DatabaseDeviationAnalyzer: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error running Database Instance Analysis: {str(e)}")
            return False
        
    def run_server_workflow(self):
        """
        Run the server extraction and analysis workflow.
        
        Returns:
            bool: True if workflow was successful, False otherwise
        """
        self.logger.info("Starting Server Workflow")
        
        try:
            # Import the server extractor
            from src.agents.server_data_extractor import ServerDataExtractor
            
            # Create and run the server extractor
            server_extractor = ServerDataExtractor(
                data_dir=self.data_dir,
                output_data_dir=self.output_dir
            )
            
            server_success = server_extractor.run()
            
            if server_success:
                server_output_file = server_extractor.save_verified_population_file()
                if server_output_file:
                    self.logger.info(f"Server extraction completed successfully. Output file: {server_output_file}")
                    
                    # Run server deviation analysis
                    server_analysis_success = self.run_server_analysis()
                    if not server_analysis_success:
                        self.logger.error("Server analysis failed")
                    
                    return True
                else:
                    self.logger.error("Failed to save server output file")
                    return False
            else:
                self.logger.error("Server extraction failed")
                return False
                
        except ImportError as e:
            self.logger.error(f"Error importing server modules: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error in server workflow: {str(e)}")
            return False
       
    def run_server_analysis(self):
        """
        Run the server deviation analysis.
        
        Returns:
            bool: True if analysis was successful, False otherwise
        """
        self.logger.info("Starting Server Deviation Analysis")
        
        try:
            # Import the analyzer
            from src.agents.server_deviation_analyzer import ServerDeviationAnalyzer
            
            server_analyzer = ServerDeviationAnalyzer(
                input_dir=self.output_dir,
                output_dir=self.output_dir
            )
            
            server_analysis_success = server_analyzer.run()
            
            if server_analysis_success:
                self.logger.info("Server Deviation Analysis completed successfully")
                return True
            else:
                self.logger.error("Server Deviation Analysis failed")
                return False
                
        except ImportError as e:
            self.logger.error(f"Error importing ServerDeviationAnalyzer: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error running Server Deviation Analysis: {str(e)}")
            return False   

    def run_url_workflow(self):
        """
        Run the URL endpoint extraction and analysis workflow.
        
        Returns:
            bool: True if workflow was successful, False otherwise
        """
        self.logger.info("Starting URL Endpoint Workflow")
        
        try:
            # Import the URL endpoint extractor
            from src.agents.url_endpoint_extractor import URLEndpointExtractor
            
            # Create and run the URL endpoint extractor
            url_extractor = URLEndpointExtractor(
                data_dir=self.data_dir,
                output_data_dir=self.output_dir
            )
            
            url_success = url_extractor.run()
            
            if url_success:
                url_output_file = url_extractor.save_verified_population_file()
                if url_output_file:
                    self.logger.info(f"URL endpoint extraction completed successfully. Output file: {url_output_file}")
                    
                    # Run URL endpoint deviation analysis
                    url_analysis_success = self.run_url_analysis()
                    if not url_analysis_success:
                        self.logger.error("URL endpoint analysis failed")
                    
                    return True
                else:
                    self.logger.error("Failed to save URL endpoint output file")
                    return False
            else:
                self.logger.error("URL endpoint extraction failed")
                return False
                
        except ImportError as e:
            self.logger.error(f"Error importing URL endpoint modules: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error in URL endpoint workflow: {str(e)}")
            return False

    def run_url_analysis(self):
        """
        Run the URL endpoint deviation analysis.
        
        Returns:
            bool: True if analysis was successful, False otherwise
        """
        self.logger.info("Starting URL Endpoint Deviation Analysis")
        
        try:
            # Import the analyzer
            from src.agents.url_endpoint_deviation_analyzer import URLEndpointDeviationAnalyzer
            
            url_analyzer = URLEndpointDeviationAnalyzer(
                input_dir=self.output_dir,
                output_dir=self.output_dir
            )
            
            url_analysis_success = url_analyzer.run()
            
            if url_analysis_success:
                self.logger.info("URL Endpoint Deviation Analysis completed successfully")
                return True
            else:
                self.logger.error("URL Endpoint Deviation Analysis failed")
                return False
                
        except ImportError as e:
            self.logger.error(f"Error importing URLEndpointDeviationAnalyzer: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error running URL Endpoint Deviation Analysis: {str(e)}")
            return False   

    def run_cloud_analysis(self):
        """
        Run the cloud resource deviation analysis.
        
        Returns:
            bool: True if analysis was successful, False otherwise
        """
        self.logger.info("Starting Cloud Resource Deviation Analysis")
        
        try:
            # Import the analyzer
            from src.agents.cloud_resource_deviation_analyzer import CloudResourceDeviationAnalyzer
            
            cloud_analyzer = CloudResourceDeviationAnalyzer(
                input_dir=self.output_dir,
                output_dir=self.output_dir
            )
            
            cloud_analysis_success = cloud_analyzer.run()
            
            if cloud_analysis_success:
                self.logger.info("Cloud Resource Deviation Analysis completed successfully")
                return True
            else:
                self.logger.error("Cloud Resource Deviation Analysis failed")
                return False
                
        except ImportError as e:
            self.logger.error(f"Error importing CloudResourceDeviationAnalyzer: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error running Cloud Resource Deviation Analysis: {str(e)}")
            return False      