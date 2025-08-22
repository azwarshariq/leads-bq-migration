import logging
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from config import BQ_PROJECT_ID, BQ_DATASET, BQ_LEADS_TABLE, BQ_RAW_DOCS_TABLE
from slack_logger import SlackLogger

logger = logging.getLogger(__name__)

class BigQueryClient:
    """Handles all BigQuery operations for the leads migration function."""
    
    def __init__(self):
        self.client = bigquery.Client()
    
    def execute_query(self, query, description="BigQuery query"):
        """Execute a BigQuery query and return results."""
        query_job = self.client.query(query)
        results = query_job.result()
        
        # Convert results to list of dictionaries
        rows = []
        for row in results:
            row_dict = {}
            for key, value in row.items():
                row_dict[key] = value
            rows.append(row_dict)
        
        return rows
    
    def get_leads_count(self):
        """Get total count of leads in the process_municipal_lead table."""
        query = f"SELECT COUNT(*) AS total_leads FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_LEADS_TABLE}`"
        return self.execute_query(query, "Get total leads count")
    
    def update_meeting_dates(self):
        """Update meeting dates based on PDF dates from raw_documents."""
        query = f"""
        UPDATE `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_LEADS_TABLE}` AS mp
        SET mp.meeting_date = CASE
            WHEN rd.pdf_date > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 28 DAY)
              THEN TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)
            ELSE rd.pdf_date
          END
        FROM (
          SELECT uploaded_gcs_path, MAX(pdf_date) AS pdf_date
          FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_DOCS_TABLE}`
          GROUP BY uploaded_gcs_path
        ) AS rd
        WHERE mp.pdf_url = rd.uploaded_gcs_path
        """
        return self.execute_query(query, "Update meeting dates based on PDF dates")
    
    def get_updated_meeting_dates_count(self):
        """Count rows with updated meeting dates."""
        query = f"""
        SELECT COUNT(*) AS updated_count
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_LEADS_TABLE}` mp
        JOIN (
          SELECT uploaded_gcs_path, MAX(pdf_date) AS pdf_date
          FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_DOCS_TABLE}`
          GROUP BY uploaded_gcs_path
        ) AS rd ON mp.pdf_url = rd.uploaded_gcs_path
        WHERE mp.meeting_date IS NOT NULL
        """
        return self.execute_query(query, "Count rows with updated meeting dates")
    
    def check_null_bytes(self):
        """Check for null bytes in various fields and return count if any found."""
        query = f"""
        SELECT 
          COUNTIF(REGEXP_CONTAINS(resolution_number, r'\\x00')) AS resolution_number_bad,
          COUNTIF(REGEXP_CONTAINS(status, r'\\x00')) AS status_bad,
          COUNTIF(REGEXP_CONTAINS(sector, r'\\x00')) AS sector_bad,
          COUNTIF(REGEXP_CONTAINS(project, r'\\x00')) AS project_bad,
          COUNTIF(REGEXP_CONTAINS(project_value, r'\\x00')) AS project_value_bad,
          COUNTIF(REGEXP_CONTAINS(timeline, r'\\x00')) AS timeline_bad,
          COUNTIF(REGEXP_CONTAINS(stakeholders, r'\\x00')) AS stakeholders_bad,
          COUNTIF(REGEXP_CONTAINS(associated_companies, r'\\x00')) AS associated_companies_bad,
          COUNTIF(REGEXP_CONTAINS(other_notes, r'\\x00')) AS other_notes_bad,
          COUNTIF(REGEXP_CONTAINS(title, r'\\x00')) AS title_bad,
          COUNTIF(REGEXP_CONTAINS(status_standardized, r'\\x00')) AS status_standardized_bad,
          COUNTIF(REGEXP_CONTAINS(pdf_url, r'\\x00')) AS pdf_url_bad,
          COUNTIF(REGEXP_CONTAINS(source_timestamp, r'\\x00')) AS source_timestamp_bad,
          COUNTIF(REGEXP_CONTAINS(agency_id, r'\\x00')) AS agency_id_bad,
          COUNTIF(REGEXP_CONTAINS(agency_name, r'\\x00')) AS agency_name_bad,
          COUNTIF(REGEXP_CONTAINS(link, r'\\x00')) AS link_bad,
          COUNTIF(REGEXP_CONTAINS(uuid, r'\\x00')) AS uuid_bad
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_LEADS_TABLE}`
        """
        return self.execute_query(query, "Check for null bytes in fields")
    
    def clean_null_bytes(self):
        """Clean null bytes from all fields in the process_municipal_lead table."""
        query = f"""
        UPDATE `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_LEADS_TABLE}`
        SET 
          resolution_number    = REGEXP_REPLACE(resolution_number, r'\\x00', ''),
          status               = REGEXP_REPLACE(status, r'\\x00', ''),
          sector               = REGEXP_REPLACE(sector, r'\\x00', ''),
          project              = REGEXP_REPLACE(project, r'\\x00', ''),
          project_value        = REGEXP_REPLACE(project_value, r'\\x00', ''),
          timeline             = REGEXP_REPLACE(timeline, r'\\x00', ''),
          stakeholders         = REGEXP_REPLACE(stakeholders, r'\\x00', ''),
          associated_companies = REGEXP_REPLACE(associated_companies, r'\\x00', ''),
          other_notes          = REGEXP_REPLACE(other_notes, r'\\x00', ''),
          title                = REGEXP_REPLACE(title, r'\\x00', ''),
          status_standardized  = REGEXP_REPLACE(status_standardized, r'\\x00', ''),
          pdf_url              = REGEXP_REPLACE(pdf_url, r'\\x00', ''),
          source_timestamp     = REGEXP_REPLACE(source_timestamp, r'\\x00', ''),
          agency_id            = REGEXP_REPLACE(agency_id, r'\\x00', ''),
          agency_name          = REGEXP_REPLACE(agency_name, r'\\x00', ''),
          link                 = REGEXP_REPLACE(link, r'\\x00', ''),
          uuid                 = REGEXP_REPLACE(uuid, r'\\x00', '')
        WHERE TRUE
        """
        return self.execute_query(query, "Clean null bytes from all fields")
    
    def migrate_to_municipal_lead_test(self):
        """Migrate data from process_municipal_lead to municipal_lead_test table."""
        query = f"""
        INSERT INTO `{BQ_PROJECT_ID}.civiq_meeting.municipal_lead_test` (
          resolution_number,
          status,
          sector,
          project,
          project_value,
          timeline,
          stakeholders,
          source_timestamp,
          associated_companies,
          other_notes,
          agency_id,
          agency_name,
          transcript_url,
          title,
          link,
          meeting_date,
          status_standardized,
          pdf_url,
          created_at,
          modified_at,
          uuid,
          more_info,
          is_active,
          audio_gcs_path
        )
        SELECT
          resolution_number,
          status,
          sector,
          project,
          project_value,
          timeline,
          stakeholders,
          source_timestamp,
          associated_companies,
          other_notes,
          agency_id,
          agency_name,
          NULL AS transcript_url,
          title,
          link,
          meeting_date,
          status_standardized,
          pdf_url,
          created_at,
          modified_at,
          uuid,
          NULL AS more_info,
          TRUE AS is_active,
          NULL AS audio_gcs_path
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_LEADS_TABLE}`
        """
        return self.execute_query(query, "Migrate data to municipal_lead_test table")
    
    def count_municipal_lead_test(self):
        """Count total records in the municipal_lead_test table."""
        query = f"SELECT COUNT(*) AS total_leads FROM `{BQ_PROJECT_ID}.civiq_meeting.municipal_lead_test`"
        return self.execute_query(query, "Count records in municipal_lead_test table")
    
    def truncate_source_table(self):
        """Truncate all records from the BQ_LEADS_TABLE."""
        query = f"DELETE FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_LEADS_TABLE}` WHERE TRUE"
        return self.execute_query(query, "Truncate BQ_LEADS_TABLE") 