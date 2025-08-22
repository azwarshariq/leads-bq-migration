import logging
from datetime import datetime
from bigquery_client import BigQueryClient
from slack_logger import SlackLogger
from config import BQ_LEADS_TABLE

logger = logging.getLogger(__name__)

class MigrationService:
    """Handles the leads migration business logic."""
    
    def __init__(self):
        self.bq_client = BigQueryClient()
    
    def run_migration(self):
        """Execute the complete leads migration process."""
        try:
            logger.info("Starting leads migration process")
            SlackLogger.migration_started()
            
            results = {}
            
            # Step 1: Get and log initial leads count
            results['initial_leads_count'] = self._step_1_get_initial_count()
            
            # Step 2: Update meeting dates
            results['meeting_date_update'] = self._step_2_update_meeting_dates()
            
            # Step 3: Check and clean null bytes
            results.update(self._step_3_handle_null_bytes())
            
            # Step 4: Count records in municipal_lead_test table
            results['initial_municipal_lead_count'] = self._step_4_count_municipal_lead()
            
            # Step 5: Migrate data to municipal_lead_test table
            results['migration_to_municipal_lead'] = self._step_5_migrate_to_municipal_lead()
            
            # Step 6: Get updated count in municipal_lead_test table
            results['final_municipal_lead_count'] = self._step_6_get_updated_municipal_lead_count()
            
            # Step 7: Truncate BQ_LEADS_TABLE if migration was successful
            results['truncate_source_table'] = self._step_7_truncate_source_table(results)
            
            # Send completion notification
            self._send_completion_notification(results)
            
            logger.info("Leads migration completed successfully")
            return results
            
        except Exception as e:
            error_msg = f"Migration failed: {str(e)}"
            logger.error(error_msg)
            SlackLogger.error(error_msg)
            raise e
    
    def _step_1_get_initial_count(self):
        """Step 1: Get initial leads count and notify."""
        initial_count = self.bq_client.get_leads_count()
        if initial_count:
            count = initial_count[0].get('total_leads', 0)
            logger.info(f"Step 1: Initial leads count: {count}")
            SlackLogger.send(f"✅ Step 1: {count:,} leads found", "INFO")
        return initial_count
    
    def _step_2_update_meeting_dates(self):
        """Step 2: Update meeting dates and notify."""
        logger.info("Step 2: Updating meeting dates...")
        result = self.bq_client.update_meeting_dates()
        SlackLogger.send("✅ Step 2: Meeting dates updated", "INFO")
        return result
    
    def _step_3_handle_null_bytes(self):
        """Step 3: Check for null bytes, clean if needed, and notify."""
        logger.info("Step 3: Checking for null bytes...")
        results = {}
        
        # Initial null bytes check
        results['null_bytes_check'] = self.bq_client.check_null_bytes()
        null_bytes_result = results['null_bytes_check']
        
        if null_bytes_result:
            total_bad_rows = sum(null_bytes_result[0].values())
            
            if total_bad_rows > 0:
                # Found null bytes - clean them
                logger.info(f"Step 3: Found {total_bad_rows} rows with null bytes, cleaning...")
                SlackLogger.send(f"⚠️ Step 3: Found {total_bad_rows} rows with null bytes", "WARNING")
                
                results['null_bytes_cleanup'] = self.bq_client.clean_null_bytes()
                
                # Recheck after cleanup
                results['null_bytes_check_after_cleanup'] = self.bq_client.check_null_bytes()
                final_check = results['null_bytes_check_after_cleanup']
                
                if final_check:
                    final_bad_rows = sum(final_check[0].values())
                    if final_bad_rows == 0:
                        SlackLogger.send("✅ Step 3: All null bytes cleaned", "INFO")
                    else:
                        SlackLogger.send(f"⚠️ Step 3: {final_bad_rows} rows still have null bytes", "WARNING")
            else:
                # No null bytes found
                SlackLogger.send("✅ Step 3: No null bytes found", "INFO")
        else:
            logger.warning("Step 3: Null bytes check returned no results")
            SlackLogger.send("⚠️ Step 3: Null bytes check failed", "WARNING")
        
        return results
    
    def _step_4_count_municipal_lead(self):
        """Step 4: Count records in municipal_lead_test table and notify."""
        logger.info("Step 4: Counting records in municipal_lead_test table...")
        count = self.bq_client.count_municipal_lead_test()
        if count:
            count_value = count[0].get('total_leads', 0)
            logger.info(f"Step 4: Initial municipal_lead_test count: {count_value}")
            SlackLogger.send(f"✅ Step 4: {count_value:,} records in target table", "INFO")
        return count
    
    def _step_5_migrate_to_municipal_lead(self):
        """Step 5: Migrate data to municipal_lead_test table and notify."""
        logger.info("Step 5: Migrating data to municipal_lead_test table...")
        result = self.bq_client.migrate_to_municipal_lead_test()
        SlackLogger.send("✅ Step 5: Data migration completed", "INFO")
        return result
    
    def _step_6_get_updated_municipal_lead_count(self):
        """Step 6: Get updated count in municipal_lead_test table and notify."""
        logger.info("Step 6: Getting updated municipal_lead_test count...")
        count = self.bq_client.count_municipal_lead_test()
        if count:
            count_value = count[0].get('total_leads', 0)
            logger.info(f"Step 6: Final municipal_lead_test count: {count_value}")
            SlackLogger.send(f"✅ Step 6: {count_value:,} records migrated", "INFO")
        return count
    
    def _step_7_truncate_source_table(self, results):
        """Step 7: Truncate BQ_LEADS_TABLE if migration was successful."""
        logger.info("Step 7: Checking if migration was successful...")
        
        # Check if migration was successful by comparing newly added records to source count
        initial_count = results.get('initial_municipal_lead_count', [])
        final_count = results.get('final_municipal_lead_count', [])
        source_count = results.get('initial_leads_count', [])
        
        if (initial_count and final_count and source_count):
            initial_main = initial_count[0].get('total_leads', 0)
            final_main = final_count[0].get('total_leads', 0)
            source_staging = source_count[0].get('total_leads', 0)
            
            # Calculate how many records were added to the main table
            records_added = final_main - initial_main
            
            logger.info(f"Step 7: Main table had {initial_main:,} records initially")
            logger.info(f"Step 7: Main table has {final_main:,} records after migration")
            logger.info(f"Step 7: Staging table had {source_staging:,} records")
            logger.info(f"Step 7: Records added to main table: {records_added:,}")
            
            if records_added == source_staging:
                # Migration successful - all staging records were added
                logger.info("Step 7: Migration successful, truncating BQ_LEADS_TABLE...")
                self.bq_client.truncate_source_table()
                SlackLogger.send(f"✅ Step 7: {BQ_LEADS_TABLE} truncated", "INFO")
                return f"{BQ_LEADS_TABLE} truncated"
            else:
                # Migration not successful - record counts don't match
                logger.warning(f"Step 7: Migration not successful - added {records_added:,} but source had {source_staging:,}")
                SlackLogger.send(f"⚠️ Step 7: Migration incomplete - {records_added:,}/{source_staging:,} records migrated", "WARNING")
                return "Migration incomplete - record count mismatch"
        else:
            # Missing data for comparison
            logger.error("Step 7: Missing data for migration success check")
            SlackLogger.send("⚠️ Step 7: Cannot verify migration success", "WARNING")
            return "Cannot verify migration success - missing data"
    
    def _send_completion_notification(self, results):
        """Send final completion notification."""
        SlackLogger.send("🎉 Migration completed successfully!", "INFO")
    
    def get_status(self):
        """Get current system status and connectivity."""
        try:
            # Check BigQuery connectivity and get current leads count
            status_check = self.bq_client.get_leads_count()
            
            return {
                'status': 'ready',
                'message': 'Leads migration function is running',
                'bigquery_connectivity': 'connected',
                'total_leads': status_check[0]['total_leads'] if status_check else 0,
                'last_check': datetime.now().isoformat()
            }
            
        except Exception as e:
            error_msg = f"Status check failed: {str(e)}"
            logger.error(error_msg)
            SlackLogger.error(error_msg)
            
            return {
                'status': 'error',
                'message': 'BigQuery connectivity check failed',
                'bigquery_connectivity': 'disconnected',
                'error': str(e),
                'last_check': datetime.now().isoformat()
            } 