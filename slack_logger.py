import requests
import logging
from datetime import datetime
from config import SLACK_WEBHOOK_URL

logger = logging.getLogger(__name__)

class SlackLogger:
    """Handles all Slack notifications for the leads migration function."""
    
    @staticmethod
    def send(message, level="INFO", data=None):
        """Send a log message to Slack."""
        if not SLACK_WEBHOOK_URL:
            logger.warning("SLACK_WEBHOOK_URL not configured")
            return
        
        try:
            color = {
                "INFO": "#36a64f",
                "WARNING": "#ffa500", 
                "ERROR": "#ff0000"
            }.get(level, "#36a64f")
            
            slack_payload = {
                "attachments": [{
                    "color": color,
                    "title": f"Leads Migration - {level}",
                    "text": message,
                    "fields": [{
                        "title": "Time",
                        "value": datetime.now().strftime("%H:%M"),
                        "short": True
                    }],
                    "footer": "Leads BQ Migration Function"
                }]
            }
            
            # Add data fields if provided
            if data:
                for key, value in data.items():
                    slack_payload["attachments"][0]["fields"].append({
                        "title": key,
                        "value": str(value)[:100] + "..." if len(str(value)) > 100 else str(value),
                        "short": False
                    })
            
            response = requests.post(
                SLACK_WEBHOOK_URL,
                json=slack_payload,
                timeout=10
            )
            
            if response.status_code != 200:
                logger.error(f"Slack API error: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Error sending Slack message: {str(e)}")
    
    @staticmethod
    def migration_started():
        """Log migration start."""
        SlackLogger.send("Migration started", "INFO")
    
    @staticmethod
    def migration_completed(total_rows):
        """Log migration completion."""
        SlackLogger.send(f"Migration completed - {total_rows:,} rows processed", "INFO")
    
    @staticmethod
    def meeting_dates_updated():
        """Log meeting date updates."""
        SlackLogger.send("Meeting dates updated", "INFO")
    
    @staticmethod
    def status_check(total_leads):
        """Log status check results."""
        SlackLogger.send(f"Total leads: {total_leads:,}", "INFO")
    
    @staticmethod
    def error(message, context=None):
        """Log errors."""
        SlackLogger.send(message, "ERROR", context) 