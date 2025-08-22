import functions_framework
from flask import Request
import json
import logging
from datetime import datetime
from migration_service import MigrationService
from slack_logger import SlackLogger
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@functions_framework.http
def leads_migration(request: Request):
    """
    HTTP Cloud Function for leads migration to BigQuery.
    
    Args:
        request (flask.Request): The request object.
        
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
    """
    # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    # Set CORS headers for the main request
    headers = {
        'Access-Control-Allow-Origin': '*'
    }

    try:
        # Get request data
        request_json = request.get_json(silent=True)
        request_args = request.args

        # Log the request
        logger.info(f"Request method: {request.method}")
        logger.info(f"Request JSON: {request_json}")
        logger.info(f"Request args: {request_args}")

        # Initialize migration service
        migration_service = MigrationService()

        if request.method == 'POST':
            # Handle POST request for migration
            if request_json:
                try:
                    # Execute migration
                    print("Migration started")
                    migration_results = migration_service.run_migration()
                    print("Migration results: ", migration_results)
                    # Extract total leads count from results
                    initial_count = migration_results.get('initial_leads_count', [])
                    total_leads = initial_count[0]['total_leads'] if initial_count else 0
                    
                    return (json.dumps(migration_results), 200, headers)
                    
                except Exception as e:
                    error_msg = f"BigQuery migration failed: {str(e)}"
                    logger.error(error_msg)
                    return (json.dumps({'error': error_msg}), 500, headers)
            else:
                error_msg = "Migration request failed - No JSON data provided"
                return (json.dumps({'error': error_msg}), 400, headers)
                
        elif request.method == 'GET':
            # Handle GET request for status
            try:
                status_response = migration_service.get_status()
                return (json.dumps(status_response), 200, headers)
                
            except Exception as e:
                error_msg = f"Status check failed: {str(e)}"
                logger.error(error_msg)
                
                status_response = {
                    'status': 'error',
                    'message': 'Status check failed',
                    'error': str(e),
                    'last_check': datetime.now().isoformat()
                }
                
                return (json.dumps(status_response), 500, headers)
        else:
            error_msg = f"Method not allowed: {request.method}"
            return (json.dumps({'error': error_msg}), 405, headers)

    except Exception as e:
        error_msg = f"Error processing request: {str(e)}"
        logger.error(error_msg)
        return (json.dumps({'error': error_msg}), 500, headers)

# Function is ready (no startup notifications sent)
if __name__ == "__main__":
    print("Function ready to run locally")
else:
    print("Function ready in Google Cloud Functions")


