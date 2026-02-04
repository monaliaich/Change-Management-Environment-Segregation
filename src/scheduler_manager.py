import logging
import time
from datetime import datetime
from src.utils.scheduler import Scheduler
import pandas as pd

class SchedulerManager:
    """
    Manager for scheduling different workflows.
    """
    def __init__(self, data_dir, output_dir, interval=5):
        """
        Initialize the scheduler manager.
        
        Args:
            data_dir: Directory containing input data files
            output_dir: Directory for output files
            interval: Interval in minutes for periodic execution
        """
        self.logger = logging.getLogger("SchedulerManager")
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.interval = interval
        self.schedulers = {}
        
        # Import the workflow manager
        from src.workflow_manager import WorkflowManager
        self.workflow_manager = WorkflowManager(data_dir, output_dir)
    
    def setup_environment_scheduler(self):
        """
        Set up the environment scheduler.
        
        Returns:
            bool: True if setup was successful, False otherwise
        """
        try:
            # Import the environment extractor
            from src.agents.extraction_logic import EnvironmentDataExtractor
            
            # Create the scheduler
            env_scheduler = Scheduler(
                agent_class=EnvironmentDataExtractor,
                data_dir=self.data_dir,
                output_dir=self.output_dir
            )
            
            # Set the interval and store the scheduler
            env_scheduler.set_interval(self.interval)
            self.schedulers['environment'] = env_scheduler
            
            return True
            
        except ImportError as e:
            self.logger.error(f"Error importing environment modules: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error setting up environment scheduler: {str(e)}")
            return False
    
    def setup_database_scheduler(self):
        """
        Set up the database scheduler.
        
        Returns:
            bool: True if setup was successful, False otherwise
        """
        try:
            # Import the database extractor
            from src.agents.database_extraction_logic import DatabaseDataExtractor
            
            # Create the scheduler
            db_scheduler = Scheduler(
                agent_class=DatabaseDataExtractor,
                data_dir=self.data_dir,
                output_dir=self.output_dir
            )
            
            # Set the interval and store the scheduler
            db_scheduler.set_interval(self.interval)
            self.schedulers['database'] = db_scheduler
            
            return True
            
        except ImportError as e:
            self.logger.error(f"Error importing database modules: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error setting up database scheduler: {str(e)}")
            return False

    def setup_server_scheduler(self):
        """
        Set up the server scheduler.
        
        Returns:
            bool: True if setup was successful, False otherwise
        """
        try:
            # Import the server extractor
            from src.agents.server_data_extractor import ServerDataExtractor
            
            # Create the scheduler
            server_scheduler = Scheduler(
                agent_class=ServerDataExtractor,
                data_dir=self.data_dir,
                output_dir=self.output_dir
            )
            
            # Set the interval and store the scheduler
            server_scheduler.set_interval(self.interval)
            self.schedulers['server'] = server_scheduler
            
            return True
            
        except ImportError as e:
            self.logger.error(f"Error importing server modules: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error setting up server scheduler: {str(e)}")
            return False    
    
    def setup_url_scheduler(self):
        """
        Set up the URL endpoint scheduler.
        
        Returns:
            bool: True if setup was successful, False otherwise
        """
        try:
            # Import the URL endpoint extractor
            from src.agents.url_endpoint_extractor import URLEndpointExtractor
            
            # Create the scheduler
            url_scheduler = Scheduler(
                agent_class=URLEndpointExtractor,
                data_dir=self.data_dir,
                output_dir=self.output_dir
            )
            
            # Set the interval and store the scheduler
            url_scheduler.set_interval(self.interval)
            self.schedulers['url'] = url_scheduler
            
            return True
            
        except ImportError as e:
            self.logger.error(f"Error importing URL endpoint modules: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error setting up URL endpoint scheduler: {str(e)}")
            return False
        
    def setup_cloud_scheduler(self):
        """
        Set up the cloud resource scheduler.
        
        Returns:
            bool: True if setup was successful, False otherwise
        """
        try:
            # Import the cloud resource extractor
            from src.agents.cloud_resource_extractor import CloudResourceExtractor
            
            # Create the scheduler
            cloud_scheduler = Scheduler(
                agent_class=CloudResourceExtractor,
                data_dir=self.data_dir,
                output_dir=self.output_dir
            )
            
            # Set the interval and store the scheduler
            cloud_scheduler.set_interval(self.interval)
            self.schedulers['cloud'] = cloud_scheduler
            
            return True
            
        except ImportError as e:
            self.logger.error(f"Error importing cloud resource modules: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error setting up cloud resource scheduler: {str(e)}")
            return False    

    def start_schedulers(self):
        """
        Start all schedulers.
        
        Returns:
            bool: True if all schedulers were started, False otherwise
        """
        success = True
        
        for name, scheduler in self.schedulers.items():
            try:
                scheduler.start()
                self.logger.info(f"Started {name} scheduler")
            except Exception as e:
                self.logger.error(f"Error starting {name} scheduler: {str(e)}")
                success = False
        
        return success
    
    def stop_schedulers(self):
        """
        Stop all schedulers.
        
        Returns:
            bool: True if all schedulers were stopped, False otherwise
        """
        success = True
        
        for name, scheduler in self.schedulers.items():
            try:
                if scheduler.running:
                    scheduler.stop()
                    self.logger.info(f"Stopped {name} scheduler")
            except Exception as e:
                self.logger.error(f"Error stopping {name} scheduler: {str(e)}")
                success = False
        
        return success
    
    def run_schedulers(self, duration=0):
        """
        Run schedulers for a specified duration or indefinitely.
        
        Args:
            duration: Duration in minutes (0 for indefinite)
            
        Returns:
            bool: True if schedulers ran successfully, False otherwise
        """
        try:
            if duration > 0:
                self.logger.info(f"Running schedulers for {duration} minutes")
                end_time = time.time() + (duration * 60)
                
                while time.time() < end_time and any(s.running for s in self.schedulers.values()):
                    self._check_and_execute_schedulers()
                    
                    # Print a simple progress indicator
                    minutes_left = int((end_time - time.time()) / 60)
                    print(f"Scheduler running. {minutes_left} minutes remaining. Press Ctrl+C to stop.", end="\r")
                    
                    # Sleep for a short time to avoid high CPU usage
                    time.sleep(10)
                
                # Stop all schedulers
                self.stop_schedulers()
                self.logger.info("Schedulers stopped after specified duration")
                
            else:
                self.logger.info("Running schedulers indefinitely. Press Ctrl+C to stop.")
                
                while any(s.running for s in self.schedulers.values()):
                    self._check_and_execute_schedulers()
                    
                    # Print a simple progress indicator
                    print(f"Scheduler running. Press Ctrl+C to stop.", end="\r")
                    
                    # Sleep for a short time to avoid high CPU usage
                    time.sleep(10)
            
            return True
            
        except KeyboardInterrupt:
            print("\nStopping schedulers...")
            self.stop_schedulers()
            self.logger.info("Schedulers stopped by user")
            return True
        except Exception as e:
            self.logger.error(f"Error running schedulers: {str(e)}")
            self.stop_schedulers()
            return False
    
    def _check_and_execute_schedulers(self):
        """
        Check if it's time to execute any schedulers and run them if needed.
        """
        now = datetime.now()
        
        # Check environment scheduler
        env_scheduler = self.schedulers.get('environment')
        if env_scheduler and env_scheduler.running and env_scheduler.next_run and now >= env_scheduler.next_run:
            # Execute environment extraction
            env_extraction_success = env_scheduler._execute_extraction()
            
            # Run environment analysis if extraction was successful
            if env_extraction_success:
                self.workflow_manager.run_environment_analysis()
        
        # Check database scheduler
        db_scheduler = self.schedulers.get('database')
        if db_scheduler and db_scheduler.running and db_scheduler.next_run and now >= db_scheduler.next_run:
            # Execute database extraction
            db_extraction_success = db_scheduler._execute_extraction()
            
            # Run database analysis if extraction was successful
            if db_extraction_success:
                self.workflow_manager.run_database_analysis()
        
        # Check server scheduler
        server_scheduler = self.schedulers.get('server')
        if server_scheduler and server_scheduler.running and server_scheduler.next_run and now >= server_scheduler.next_run:
            # Execute server extraction
            server_extraction_success = server_scheduler._execute_extraction()
            
            # Run server analysis if extraction was successful
            if server_extraction_success:
                self.workflow_manager.run_server_analysis()
        
        # Check URL endpoint scheduler
        url_scheduler = self.schedulers.get('url')
        if url_scheduler and url_scheduler.running and url_scheduler.next_run and now >= url_scheduler.next_run:
            # Execute URL endpoint extraction
            url_extraction_success = url_scheduler._execute_extraction()
            
            # Run URL endpoint analysis if extraction was successful
            if url_extraction_success:
                self.workflow_manager.run_url_analysis()
        
        # Check cloud resource scheduler
        cloud_scheduler = self.schedulers.get('cloud')
        if cloud_scheduler and cloud_scheduler.running and cloud_scheduler.next_run and now >= cloud_scheduler.next_run:
            # Execute cloud resource extraction
            cloud_extraction_success = cloud_scheduler._execute_extraction()
            
            # Run cloud resource analysis if extraction was successful
            if cloud_extraction_success:
                self.workflow_manager.run_cloud_analysis()
        
        # Log status of each scheduler
        for name, scheduler in self.schedulers.items():
            if scheduler.running:
                status = scheduler.get_status()
                self.logger.info(f"{name.capitalize()} scheduler status: {status['status']}")
                if status['next_run']:
                    self.logger.info(f"Next {name} run at: {status['next_run']}")