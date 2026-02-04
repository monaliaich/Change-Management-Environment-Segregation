import os
import sys
import argparse
import logging
from pathlib import Path

# Add the src directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def setup_logging():
    """Set up logging configuration for the main application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("environment_data_extraction.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger("Main")

def main():
    """Main function to orchestrate the data extraction and analysis."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Data Extraction and Analysis')
    
    parser.add_argument('--mode', choices=['run', 'schedule'], default='run',
                      help='Operation mode: run (single execution) or schedule (periodic execution)')
    
    parser.add_argument('--interval', type=int, default=5,
                      help='Interval in minutes for periodic execution (default: 5)')
    
    parser.add_argument('--duration', type=int, default=60,
                      help='Duration in minutes to run the scheduler (default: 60, 0 for indefinite)')
    
    parser.add_argument('--data-dir', 
                        default='src/data/input',
                        help='Directory containing input data files')
    
    parser.add_argument('--output-dir', 
                        default='src/data/output',
                        help='Directory for output files')
    
    # Process selection arguments
    parser.add_argument('--process', choices=['env', 'db', 'server', 'url', 'cloud', 'all'], default='env',
                      help='Process to run: env (Environment Register), db (Database), or server(Server), or url(Url), or cloud(Cloud) or all (both)')
    
    args = parser.parse_args()
    
    logger = setup_logging()
    logger.info("Starting Data Extraction and Analysis")
    
    # Determine which processes to run
    run_env_process = args.process in ['env', 'all']
    run_db_process = args.process in ['db', 'all']
    run_server_process = args.process in ['server', 'all']
    run_url_process = args.process in ['url', 'all']
    run_cloud_process = args.process in ['cloud', 'all']
    
    # Handle different modes
    if args.mode == 'run':
        # Single execution mode
        logger.info("Running in single execution mode")
        
        # Import the workflow manager
        from src.workflow_manager import WorkflowManager
        
        # Create workflow manager
        workflow_manager = WorkflowManager(
            data_dir=args.data_dir,
            output_dir=args.output_dir
        )
        
        # Run selected processes
        success = True
        
        if run_env_process:
            logger.info("Running Environment Register process")
            env_success = workflow_manager.run_environment_workflow()
            if not env_success:
                logger.error("Environment workflow failed")
                success = False
            else:
                logger.info("Environment Register process completed successfully")
        
        if run_db_process:
            logger.info("Running Database process")
            db_success = workflow_manager.run_database_workflow()
            if not db_success:
                logger.error("Database workflow failed")
                success = False
            else:
                logger.info("Database process completed successfully")

        if run_server_process:
            logger.info("Running Server process")
            server_success = workflow_manager.run_server_workflow()
            if not server_success:
                logger.error("Server workflow failed")
                success = False
            else:
                logger.info("Server process completed successfully")   

        if run_url_process:
            logger.info("Running URL Endpoint process")
            url_success = workflow_manager.run_url_workflow()
            if not url_success:
                logger.error("URL Endpoint workflow failed")
                success = False
            else:
                logger.info("URL Endpoint process completed successfully")     

        if run_cloud_process:
            logger.info("Running Cloud Resource process")
            cloud_success = workflow_manager.run_cloud_workflow()
            if not cloud_success:
                logger.error("Cloud Resource workflow failed")
                success = False
            else:
                logger.info("Cloud Resource process completed successfully")                
        
        return success
        
    elif args.mode == 'schedule':
        # Periodic execution mode
        logger.info(f"Running in periodic execution mode with {args.interval} minute interval")
        
        # Import the scheduler manager
        from src.scheduler_manager import SchedulerManager
        
        # Create scheduler manager
        scheduler_manager = SchedulerManager(
            data_dir=args.data_dir,
            output_dir=args.output_dir,
            interval=args.interval
        )
        
        # Set up selected schedulers
        if run_env_process:
            logger.info("Setting up Environment Register scheduler")
            env_setup_success = scheduler_manager.setup_environment_scheduler()
            if not env_setup_success:
                logger.error("Failed to set up environment scheduler")
        
        if run_db_process:
            logger.info("Setting up Database scheduler")
            db_setup_success = scheduler_manager.setup_database_scheduler()
            if not db_setup_success:
                logger.error("Failed to set up database scheduler")

        if run_server_process:
            logger.info("Setting up Server scheduler")
            server_setup_success = scheduler_manager.setup_server_scheduler()
            if not server_setup_success:
                logger.error("Failed to set up server scheduler")   

        if run_url_process:
            logger.info("Setting up URL Endpoint scheduler")
            url_setup_success = scheduler_manager.setup_url_scheduler()
            if not url_setup_success:
                logger.error("Failed to set up URL endpoint scheduler")     

        if run_cloud_process:
            logger.info("Setting up Cloud Resource scheduler")
            cloud_setup_success = scheduler_manager.setup_cloud_scheduler()
            if not cloud_setup_success:
                logger.error("Failed to set up Cloud Resource scheduler")                
        
        # Start all schedulers
        logger.info("Starting schedulers")
        start_success = scheduler_manager.start_schedulers()
        if not start_success:
            logger.error("Failed to start some schedulers")
        
        # Run schedulers for specified duration or indefinitely
        logger.info(f"Running schedulers for {args.duration if args.duration > 0 else 'indefinite'} minutes")
        run_success = scheduler_manager.run_schedulers(args.duration)
        
        return run_success
    
    logger.info("Data Extraction and Analysis completed")
    return True

if __name__ == "__main__":
    try:
        success = main()
        exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nProgram stopped by user.")
        exit(0)
    except Exception as e:
        print(f"Error: {str(e)}")
        exit(1)