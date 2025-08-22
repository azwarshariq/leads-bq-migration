import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Slack configuration
SLACK_WEBHOOK_URL = os.environ.get('LEADS_UPSERTION_BQ_SLACK_URL')

# BigQuery configuration
BQ_PROJECT_ID = 'civiq-prod-1'
BQ_DATASET = 'civiq_meeting'
BQ_LEADS_TABLE = 'municipal_lead_copy'
BQ_RAW_DOCS_TABLE = 'raw_documents'

# Logging configuration
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO') 