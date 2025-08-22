# Leads BigQuery Migration Function

A modular Google Cloud Function for migrating and updating leads data in BigQuery.

## 🏗️ Architecture

The codebase is organized into clean, modular components:

### Core Modules

- **`main.py`** - Main HTTP function entry point
- **`config.py`** - Configuration and environment variables
- **`slack_logger.py`** - Slack notification handling
- **`bigquery_client.py`** - BigQuery operations and queries
- **`migration_service.py`** - Business logic for migrations

### Benefits of Modular Structure

- ✅ **Separation of Concerns** - Each module has a single responsibility
- ✅ **Easy Testing** - Test individual components in isolation
- ✅ **Maintainable** - Clear code organization and easy to modify
- ✅ **Reusable** - Components can be reused in other functions
- ✅ **Clean Dependencies** - Clear import structure

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Set Environment Variables
Create a `.env` file:
```bash
LEADS_UPSERTION_BQ_SLACK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### 3. Run Locally
```bash
# Using Functions Framework
functions-framework --target=leads_migration --port=8080

# Or test the modular structure
python test_local.py
```

## 📋 API Endpoints

### GET `/` - Status Check
Returns system status and current leads count.

**Response:**
```json
{
  "status": "ready",
  "bigquery_connectivity": "connected",
  "total_leads": 406705,
  "last_check": "2025-08-22T10:15:..."
}
```

### POST `/` - Run Migration
Executes the complete leads migration process.

**Response:**
```json
{
  "initial_leads_count": {...},
  "meeting_date_update": {...},
  "updated_meeting_dates_count": {...},
  "final_leads_count": {...}
}
```

## 🔧 Migration Process

The migration runs 4 sequential steps:

1. **Initial Count** - Get current leads count
2. **Update Meeting Dates** - Update based on PDF dates (28-day future limit)
3. **Verification Count** - Count updated meeting dates
4. **Final Count** - Get final leads count

## 📱 Slack Notifications

Clean, minimal notifications for:
- Migration started
- Meeting dates updated
- Migration completed with row counts
- Status checks (total leads)
- Errors (detailed for debugging)

## 🧪 Testing

### Local Testing
```bash
python test_local.py
```

### Manual Testing
```bash
# Test GET endpoint
curl http://localhost:8080/

# Test POST endpoint
curl -X POST http://localhost:8080/ -H "Content-Type: application/json" -d '{}'
```

## 📁 File Structure

```
leads-bq-migration/
├── main.py                 # Main HTTP function
├── config.py              # Configuration
├── slack_logger.py        # Slack notifications
├── bigquery_client.py     # BigQuery operations
├── migration_service.py   # Business logic
├── test_local.py          # Local testing
├── requirements.txt       # Dependencies
└── README.md             # This file
```

## 🔒 Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `LEADS_UPSERTION_BQ_SLACK_URL` | Slack webhook URL for notifications | Yes |
| `GOOGLE_APPLICATION_CREDENTIALS` | GCP service account key path | For BigQuery access |

## 🚀 Deployment

Deploy to Google Cloud Functions:

```bash
gcloud functions deploy leads-migration \
  --runtime python39 \
  --trigger-http \
  --entry-point leads_migration \
  --source . \
  --allow-unauthenticated
```

## 📊 Monitoring

- **Console Logs** - Detailed execution logs
- **Slack Notifications** - Real-time operation updates
- **BigQuery Results** - Query execution results
- **Error Handling** - Comprehensive error logging

## 🔄 Adding New Queries

To add new BigQuery queries:

1. **Add method to `BigQueryClient`** in `bigquery_client.py`
2. **Update `MigrationService.run_migration()`** in `migration_service.py`
3. **Test locally** with `python test_local.py`

The modular structure makes it easy to extend functionality!