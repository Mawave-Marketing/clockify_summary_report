# Clockify to BigQuery ETL Pipeline

A serverless ETL pipeline that extracts time tracking data from Clockify and loads it into Google BigQuery for analysis and reporting.

## Overview

This project implements a microservices-based data pipeline using Google Cloud Functions to sync data from Clockify (a time tracking SaaS platform) to BigQuery. The pipeline fetches four main data types:

- **Summary Time Entry Reports** - Aggregated time entries by user, project, and tag
- **Clients** - Customer/client master data
- **Projects** - Project master data with billing rates and estimates
- **Users** - Workspace user profiles and settings

Each data type is processed by an independent Cloud Function that can be deployed and scaled separately.

## Architecture

### Components

```
┌─────────────────┐
│  Cloud Scheduler│
│   (Pub/Sub)     │
└────────┬────────┘
         │ Triggers
         ▼
┌─────────────────────────────────────────────────────────┐
│              Cloud Functions (Python 3.12)               │
├──────────────┬──────────────┬──────────────┬───────────┤
│   Summary    │   Clients    │   Projects   │   Users   │
│   Function   │   Function   │   Function   │  Function │
└──────┬───────┴──────┬───────┴──────┬───────┴─────┬─────┘
       │              │              │             │
       │ Fetch Data   │              │             │
       ▼              ▼              ▼             ▼
┌──────────────────────────────────────────────────────────┐
│              Clockify API (reports.api + api)            │
└──────────────────────────────────────────────────────────┘
       │              │              │             │
       │ Transform    │              │             │
       ▼              ▼              ▼             ▼
┌──────────────────────────────────────────────────────────┐
│          Google Cloud Storage (Parquet Files)            │
└──────────────────────────────────────────────────────────┘
       │              │              │             │
       │ Load & Merge │              │             │
       ▼              ▼              ▼             ▼
┌──────────────────────────────────────────────────────────┐
│           BigQuery (dl_clockify dataset)                 │
│  ┌────────────────────────────────────────────────────┐  │
│  │ • summary_time_entry_report                        │  │
│  │ • clients                                           │  │
│  │ • projects                                          │  │
│  │ • users                                             │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

### Technology Stack

- **Runtime**: Python 3.12
- **Cloud Platform**: Google Cloud Platform (GCP)
- **Compute**: Cloud Functions Gen 2
- **Storage**: Google Cloud Storage (Parquet format)
- **Data Warehouse**: BigQuery
- **Orchestration**: Cloud Pub/Sub + Cloud Scheduler
- **CI/CD**: GitHub Actions
- **Key Libraries**: pandas, google-cloud-storage, google-cloud-bigquery, pyarrow

## Data Flow

### Common Flow Pattern

All four functions follow a similar ETL pattern:

1. **Trigger**: Pub/Sub message received from Cloud Scheduler
2. **Extract**: Fetch data from Clockify API with pagination
3. **Transform**: Convert JSON to pandas DataFrame, flatten nested structures, clean column names
4. **Stage**: Save as Parquet file locally
5. **Upload**: Upload Parquet to Google Cloud Storage with retry logic
6. **Load**: Load from GCS to temporary BigQuery table
7. **Merge**: Perform UPSERT operation into main table (updates existing, inserts new)
8. **Cleanup**: Delete temporary table

### 1. Summary Time Entry Report Flow

**Function**: [src/summary/main.py](src/summary/main.py)

**Pub/Sub Topic**: `clockify-summary-report`

**Detailed Flow**:

```
1. Fetch 12 weeks of data (day-by-day to respect API limits)
   ├─ Date Range: Today - 84 days to Today
   └─ Request delay: 1 second between API calls

2. For each day:
   ├─ Request summary report with grouping: USER → PROJECT → TAG
   ├─ Parse nested JSON structure:
   │  └─ Extract: user_id, project_id, client_id, tag_id, names, duration
   ├─ Convert duration: milliseconds → decimal hours
   └─ Accumulate records in batch

3. Every 4 weeks (batch processing):
   ├─ Convert accumulated records to DataFrame
   ├─ Upload to GCS: gs://{bucket}/clockify_data/{date}/summary_report_batch_{n}.parquet
   ├─ Load to BigQuery temp table
   ├─ MERGE into summary_time_entry_report:
   │  └─ Match on: user_id + project_id + client + tags + date
   └─ Delete temp table

4. Repeat until all 12 weeks processed
```

**Key Features**:
- Batch processing (4-week chunks) to handle large datasets
- Day-by-day fetching to work within API rate limits
- Extracts both entity IDs and names for flexible joining
- Converts German time durations to decimal hours

**Output Schema** (12 fields):
- `user`, `user_id`, `project`, `project_id`, `client`, `client_id`
- `tags`, `tag_id`, `date`, `duration_decimal`, `amount_decimal`, `import_timestamp`

### 2. Clients Data Flow

**Function**: [src/clients/main.py](src/clients/main.py)

**Pub/Sub Topic**: `clockify-clients`

**Detailed Flow**:

```
1. Fetch all clients (paginated, 50 per page)
   └─ Endpoint: /api/v1/workspaces/{workspace_id}/clients

2. Transform:
   ├─ Flatten client objects
   ├─ Add import_timestamp
   └─ Clean column names

3. Upload to GCS:
   └─ Path: gs://{bucket}/clockify_data/{date}/clients.parquet

4. Load to BigQuery:
   ├─ Create temp table: dl_clockify.clients_temp
   ├─ MERGE into dl_clockify.clients
   │  └─ Match on: id
   └─ Delete temp table
```

**Output Schema** (7 fields):
- `id`, `name`, `workspaceId`, `archived`, `address`, `note`, `import_timestamp`

### 3. Projects Data Flow

**Function**: [src/projects/main.py](src/projects/main.py)

**Pub/Sub Topic**: `clockify-projects`

**Detailed Flow**:

```
1. Fetch all projects (paginated)
   └─ Endpoint: /api/v1/workspaces/{workspace_id}/projects

2. Transform with complex flattening:
   ├─ Flatten hourlyRate: {amount, currency} → hourlyRate_amount, hourlyRate_currency
   ├─ Flatten estimate: {estimate, type} → estimate_estimate, estimate_type
   ├─ Flatten timeEstimate: {estimate, type, ...} → timeEstimate_*
   ├─ Extract first membership from array → membership_*
   └─ Clean all column names

3. Upload to GCS:
   └─ Path: gs://{bucket}/clockify_data/{date}/projects.parquet

4. Load to BigQuery:
   ├─ MERGE into dl_clockify.projects
   │  └─ Match on: id
   └─ Update existing rows, insert new
```

**Output Schema** (26 fields):
- Basic: `id`, `name`, `clientId`, `workspaceId`, `archived`, `billable`, `public`, `color`
- Rates: `hourlyRate_amount`, `hourlyRate_currency`
- Estimates: `estimate_estimate`, `estimate_type`, `timeEstimate_estimate`, etc.
- Membership: `membership_userId`, `membership_hourlyRate_amount`, etc.
- Timestamps: `costRate`, `budgetEstimate`, `import_timestamp`

### 4. Users Data Flow

**Function**: [src/users/main.py](src/users/main.py)

**Pub/Sub Topic**: `clockify-users`

**Detailed Flow**:

```
1. Fetch all users (paginated)
   └─ Endpoint: /api/v1/workspaces/{workspace_id}/users

2. Transform with nested flattening:
   ├─ Extract first membership from memberships array
   ├─ Flatten memberships → memberships_*
   ├─ Flatten settings → settings_*
   ├─ Flatten hourlyRate → memberships_hourlyRate_*
   └─ Clean column names

3. Upload to GCS:
   └─ Path: gs://{bucket}/clockify_data/{date}/users.parquet

4. Load to BigQuery:
   ├─ MERGE into dl_clockify.users
   │  └─ Match on: id
   └─ Update/insert accordingly
```

**Output Schema** (26 fields):
- Profile: `id`, `email`, `name`, `activeWorkspace`, `defaultWorkspace`, `profilePicture`, `status`
- Membership: `memberships_userId`, `memberships_hourlyRate_amount`, `memberships_costRate_amount`, etc.
- Settings: `settings_weekStart`, `settings_timeZone`, `settings_timeFormat`, etc.
- Metadata: `customFields`, `import_timestamp`

## Setup and Configuration

### Prerequisites

1. **Google Cloud Project** with the following APIs enabled:
   - Cloud Functions API
   - Cloud Storage API
   - BigQuery API
   - Cloud Pub/Sub API
   - Cloud Scheduler API (for triggers)

2. **Clockify Account** with:
   - API Key (Settings → API)
   - Workspace ID

3. **Service Account** with permissions:
   - Cloud Functions Developer
   - Storage Object Admin
   - BigQuery Data Editor
   - BigQuery Job User

### Environment Variables

Each Cloud Function requires these environment variables:

| Variable | Description | Example |
|----------|-------------|---------|
| `CLOCKIFY_API_KEY` | Clockify API authentication key | `NWYwZGIyZmY...` |
| `CLOCKIFY_WORKSPACE_ID` | Target Clockify workspace ID | `5f0db2ff...` |
| `GCP_PROJECT_ID` | Google Cloud project ID | `circular-ether-368213` |
| `GCS_BUCKET_NAME` | Cloud Storage bucket for staging | `clockify-data-staging` |

### BigQuery Setup

The pipeline expects a BigQuery dataset named `dl_clockify`:

```sql
-- Create dataset
CREATE SCHEMA IF NOT EXISTS dl_clockify
OPTIONS (
  location = 'europe-west3',
  description = 'Clockify time tracking data'
);

-- Tables are created automatically by the functions
```

### Cloud Storage Setup

Create a GCS bucket for staging Parquet files:

```bash
gsutil mb -l europe-west3 gs://clockify-data-staging
```

### Pub/Sub Topics

Create Pub/Sub topics for triggering each function:

```bash
gcloud pubsub topics create clockify-summary-report
gcloud pubsub topics create clockify-clients
gcloud pubsub topics create clockify-projects
gcloud pubsub topics create clockify-users
```

### Cloud Scheduler (Optional)

Set up scheduled triggers:

```bash
# Summary report - Daily at 6 AM
gcloud scheduler jobs create pubsub clockify-summary-daily \
  --location=europe-west3 \
  --schedule="0 6 * * *" \
  --topic=clockify-summary-report \
  --message-body='{"trigger":"scheduled"}'

# Master data - Weekly on Mondays at 5 AM
gcloud scheduler jobs create pubsub clockify-master-data-weekly \
  --location=europe-west3 \
  --schedule="0 5 * * 1" \
  --topic=clockify-clients \
  --message-body='{"trigger":"scheduled"}'

# Similar for projects and users
```

## Deployment

### Automated Deployment (GitHub Actions)

The project includes a CI/CD pipeline that deploys functions automatically:

**Trigger Conditions**:
- Push to `main` branch → Deploys all functions
- Manual workflow dispatch → Deploy specific function

**Workflow File**: [.github/workflows/deploy_cloudfunction.yml](.github/workflows/deploy_cloudfunction.yml)

**Required GitHub Secrets**:
```
CLOCKIFY_API_KEY
CLOCKIFY_WORKSPACE_ID
GCP_PROJECT_ID
GCS_BUCKET_NAME
WORKLOAD_IDENTITY_PROVIDER
GCP_SERVICE_ACCOUNT
```

**Deployment Steps**:
1. Push code to `main` branch
2. GitHub Actions authenticates via Workload Identity Federation
3. Each function is deployed in parallel jobs
4. Functions are updated with new code and environment variables

### Manual Deployment

Deploy individual functions using `gcloud`:

```bash
# Deploy summary report function
cd src/summary
cp ../utils.py .
gcloud functions deploy clockify-summary-report \
  --gen2 \
  --runtime=python312 \
  --region=europe-west3 \
  --source=. \
  --entry-point=main \
  --trigger-topic=clockify-summary-report \
  --timeout=540s \
  --memory=512MB \
  --max-instances=1 \
  --set-env-vars CLOCKIFY_API_KEY=$CLOCKIFY_API_KEY,CLOCKIFY_WORKSPACE_ID=$CLOCKIFY_WORKSPACE_ID,GCP_PROJECT_ID=$GCP_PROJECT_ID,GCS_BUCKET_NAME=$GCS_BUCKET_NAME

# Similar for clients, projects, users functions
```

## Project Structure

```
clockify_summary_report/
├── .github/
│   └── workflows/
│       └── deploy_cloudfunction.yml    # CI/CD pipeline
├── src/
│   ├── utils.py                        # Shared utilities
│   ├── requirements.txt                # Common dependencies
│   │
│   ├── summary/
│   │   ├── main.py                     # Summary report function
│   │   └── requirements.txt
│   │
│   ├── clients/
│   │   ├── main.py                     # Clients function
│   │   └── requirements.txt
│   │
│   ├── projects/
│   │   ├── main.py                     # Projects function
│   │   └── requirements.txt
│   │
│   └── users/
│       ├── main.py                     # Users function
│       └── requirements.txt
└── README.md
```

### Shared Utilities (utils.py)

The [src/utils.py](src/utils.py) module provides common functions used by all Cloud Functions:

**Key Functions**:

- `get_clockify_headers()` - Returns API authentication headers
- `clean_column_name(name)` - Normalizes column names (lowercase, no special chars)
- `upload_to_gcs(df, filename, bucket_name, project_id, folder)` - Uploads DataFrame to GCS as Parquet with retry logic
- `load_to_bigquery(gcs_uri, table_id, temp_table_id, schema, project_id, merge_keys)` - Loads data from GCS to BigQuery with MERGE operation

**Features**:
- Retry logic with exponential backoff (3 attempts) for GCS uploads
- Automatic BigQuery schema creation
- UPSERT operations to prevent duplicates
- Comprehensive error handling and logging

## Data Models

### BigQuery Tables

#### 1. summary_time_entry_report

Grain: User × Project × Client × Tag × Date

| Column | Type | Description |
|--------|------|-------------|
| user | STRING | User name |
| user_id | STRING | User ID |
| project | STRING | Project name |
| project_id | STRING | Project ID |
| client | STRING | Client name |
| client_id | STRING | Client ID |
| tags | STRING | Tag name |
| tag_id | STRING | Tag ID |
| date | DATE | Report date |
| duration_decimal | FLOAT64 | Hours worked (decimal) |
| amount_decimal | FLOAT64 | Billable amount |
| import_timestamp | TIMESTAMP | Data import time |

**Merge Keys**: `user_id`, `project_id`, `client`, `tags`, `date`

#### 2. clients

Grain: One row per client

| Column | Type | Description |
|--------|------|-------------|
| id | STRING | Client ID (PK) |
| name | STRING | Client name |
| workspaceId | STRING | Workspace ID |
| archived | BOOLEAN | Archived status |
| address | STRING | Client address |
| note | STRING | Notes |
| import_timestamp | TIMESTAMP | Data import time |

**Merge Keys**: `id`

#### 3. projects

Grain: One row per project

| Column | Type | Description |
|--------|------|-------------|
| id | STRING | Project ID (PK) |
| name | STRING | Project name |
| clientId | STRING | Client ID (FK) |
| workspaceId | STRING | Workspace ID |
| archived | BOOLEAN | Archived status |
| billable | BOOLEAN | Billable flag |
| hourlyRate_amount | FLOAT64 | Hourly rate |
| hourlyRate_currency | STRING | Currency code |
| estimate_estimate | STRING | Time estimate |
| membership_userId | STRING | Member user ID |
| ... | ... | 26 total fields |
| import_timestamp | TIMESTAMP | Data import time |

**Merge Keys**: `id`

#### 4. users

Grain: One row per user

| Column | Type | Description |
|--------|------|-------------|
| id | STRING | User ID (PK) |
| email | STRING | Email address |
| name | STRING | Full name |
| status | STRING | User status |
| settings_timeZone | STRING | User timezone |
| memberships_hourlyRate_amount | FLOAT64 | Hourly rate |
| ... | ... | 26 total fields |
| import_timestamp | TIMESTAMP | Data import time |

**Merge Keys**: `id`

## Development

### Local Development Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd clockify_summary_report
```

2. Install dependencies:
```bash
pip install -r src/requirements.txt
```

3. Set environment variables:
```bash
export CLOCKIFY_API_KEY="your-api-key"
export CLOCKIFY_WORKSPACE_ID="your-workspace-id"
export GCP_PROJECT_ID="your-project-id"
export GCS_BUCKET_NAME="your-bucket-name"
```

4. Authenticate with GCP:
```bash
gcloud auth application-default login
```

### Testing Functions Locally

Use the Functions Framework to test locally:

```bash
# Test summary function
cd src/summary
cp ../utils.py .
functions-framework --target=main --signature-type=event

# Trigger with curl
curl localhost:8080 \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "data": {
      "data": "eyJ0cmlnZ2VyIjoidGVzdCJ9"
    }
  }'
```

### Code Style

The codebase follows these conventions:
- Snake_case for variables and functions
- Clear, descriptive function names
- Comprehensive logging for debugging
- Type hints where beneficial
- Modular, reusable utility functions

## Monitoring and Troubleshooting

### Logging

All functions write logs to Cloud Logging. View logs:

```bash
gcloud functions logs read clockify-summary-report --region=europe-west3 --limit=50
```

### Common Issues

**1. SSL/Connection Errors during GCS Upload**
- Solution: Retry logic with exponential backoff is implemented (3 attempts)
- Check logs for `Retrying GCS upload` messages

**2. Function Timeout**
- Summary function: 540s timeout (9 minutes)
- Other functions: 300s timeout (5 minutes)
- If processing takes longer, reduce date range or increase timeout

**3. API Rate Limiting**
- 1-second delay implemented between requests
- Batch processing reduces number of API calls

**4. BigQuery Merge Conflicts**
- Merge keys ensure proper UPSERT behavior
- Recent change: Summary uses `client` and `tags` names (not IDs) in merge key

### Performance Monitoring

Monitor function performance in Cloud Console:
- Execution time
- Memory usage
- Error rate
- Invocation count

## Data Quality Features

1. **Idempotency**: Functions can be run multiple times safely (MERGE operations)
2. **Retry Logic**: GCS uploads retry 3 times with exponential backoff
3. **Batch Processing**: Large datasets processed in chunks to avoid timeouts
4. **Schema Enforcement**: Explicit BigQuery schemas prevent data type issues
5. **Rate Limiting**: API delays prevent throttling
6. **Comprehensive Logging**: Detailed logs for debugging

## Architecture Decisions

### Why Separate Functions?

1. **Independent Scaling**: Each data type can scale independently
2. **Fault Isolation**: Failure in one function doesn't affect others
3. **Flexible Scheduling**: Different refresh frequencies (daily vs. weekly)
4. **Easier Maintenance**: Changes to one function don't risk others
5. **Cost Optimization**: Only run what's needed when needed

### Why Parquet Format?

1. **Columnar Storage**: Efficient for analytics workloads
2. **Compression**: Reduces storage costs and transfer time
3. **Schema Preservation**: Type information embedded in file
4. **BigQuery Native**: Direct import without conversion

### Why MERGE Instead of Replace?

1. **Incremental Updates**: Only changed rows are updated
2. **Cost Efficiency**: Less data processed in BigQuery
3. **Audit Trail**: Can track when records were last updated
4. **Flexibility**: Supports both updates and inserts

## Future Enhancements

Potential improvements:
- [ ] Add data validation checks before BigQuery load
- [ ] Implement change data capture (CDC) for incremental loads
- [ ] Add dbt transformations for data modeling
- [ ] Create Looker/Tableau dashboards
- [ ] Add alerting for function failures
- [ ] Implement data quality tests
- [ ] Add support for multiple Clockify workspaces

## Contributing

1. Create a feature branch
2. Make changes and test locally
3. Ensure code follows existing patterns
4. Push to GitHub (triggers deployment to `main`)
5. Monitor Cloud Functions logs for issues

## License

[Add your license information here]

## Support

For issues or questions:
- Check Cloud Functions logs for error details
- Review Clockify API documentation: https://docs.clockify.me/
- Review BigQuery documentation: https://cloud.google.com/bigquery/docs

---

**Last Updated**: 2025-11-27
**Version**: 2.0
**Maintained By**: [Your Team/Name]
