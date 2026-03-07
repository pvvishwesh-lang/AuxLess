# AuxLess Backend Pipelines

### ABOUT

Implemented an end-to-end data batch and streaming pipeline that:

1: Fetches playlist data from external APIs (YouTube/Spotify metadata via user refresh tokens)

2: Processes and validates the data using Dataflow (Apache Beam)

3: Stores intermediate and final outputs in Google Cloud Storage (GCS)

4: Tracks session and user state in Firestore

5: Generates data schema, statistics, and bias metrics

6: Produces combined validated and invalid datasets

7: Ensures reproducibility, logging, testing, and versioning

8: Triggers a streaming pipeline at the end to continously integrate user feedback

### ARCHITECTURE

Firestore Session document with status='pending' created containing user refresh token

Cloud Function triggered, which sends message to Cloud Pub/Sub containing session details

Cloud Pub/Sub pushes message to Cloud Run endpoint 

Cloud Run endpoint parses message and runs pipelines per user

Pipelines pull user playlist data using custom ReadFromAPI class, performs schema validation, bias metrics and saves data in GCS

Updates Firestore status to 'done'


### PIPELINE ORCHESTRATION
This pipeline uses Cloud Run as the orchestration layer in place of Airflow, 
which satisfies the requirement for a similar orchestration tool.

| Airflow Concept     | Implementation                          |
|---------------------|-----------------------------------------|
| DAG                 | `run_for_session()` in Cloud Run        |
| Task dependencies   | Sequential calls with error handling    |
| Triggers            | Pub/Sub events                          |
| Workers             | Dataflow jobs per user                  |
| Monitoring          | Cloud Logging + Slack alerts            |
| Retry logic         | `retry_utils.py` exponential backoff    |
| Task status         | Firestore session status tracking       |


### SETUP
#### Clone Repo
git clone <repo_url>
cd <repo_name>

#### Activate GCP account and enable Cloud Run, Cloud Storage, Firestore, Pub/Sub Api's

#### Deploy DockerFile and cloudbuild.yaml files to Cloud Run, make sure to add required Environment variables(ClientID,ClientSecretID,ProjectID,FirestoreDB,REDIRECT_URIS, TokenURI)

#### Create a new Firestore db and a collection, add new session ids with structures:
status: (string)(done,pending,error,running)
users:(string):(array) 
0:(map) 
isactive: (boolean) (true/false)
last_active: (timestamp) 
refresh_token: (string) Your Refresh Token
user_id: (string) Your ID

### DATA ACQUISITION

Data is fetched per user using refresh tokens stored in Firestore.
Each user pipeline retrieves playlist metadata and writes structured CSV outputs to GCS.
Reproducibility is ensured via:
requirements.txt
Fixed schema
Deterministic file combination order

### DATA PREPROCESSING
#### Preprocessing includes:
Null handling
Type casting

#### Feature engineering:
trackTimeSeconds
like_to_view_ratio
comment_to_view_ratio
Genre and country normalization
Invalid record routing
Modular Beam transforms enable reuse and testing

### DATA VALIDATION & SCHEMA
#### Schema checks:
Required columns present
Numeric fields valid
Ratios within expected ranges
Invalid rows are written to a separate GCS path for auditing

#### Statistics generated:
Record counts
Null percentages
Distribution by genre and country

### BIAS DETECTION 
#### Bias is evaluated using data slicing across:
genre
country

### BIAS MITIGATION
Detected bias is mitigated using two techniques:
- **Upsampling**: Slices below 5% representation are oversampled to reach the threshold
- **Downsampling**: Slices above 60% dominance are reduced to that cap
A mitigation report is saved to GCS documenting before/after proportions per slice, 
which slices were adjusted, and trade-offs made (overfitting risk vs representation equity)

### RUNNING THE PIPELINE
1. Create a Firestore session document with status='pending' and user refresh tokens
2. The Cloud Function triggers automatically on document creation
3. Monitor progress via Cloud Logging or Firestore session status field
4. Outputs are written to GCS under Final_Output/{session_id}_*

#### Metrics per slice:
Mean like/view ratio
Mean comment/view ratio
Record counts

### DATA VERSIONING
Data versioning is handled through GCS path conventions. Each session's outputs 
are namespaced by session_id, ensuring every pipeline run produces isolated, 
traceable artifacts:

    Final_Output/{session_id}_combined_valid.csv
    Final_Output/{session_id}_combined_invalid.csv
    Final_Output/{session_id}_bias_metrics/{session_id}_bias_metrics.json
    Final_Output/{session_id}_schema_report/{session_id}_schema_report.json
    Final_Output/{session_id}_mitigated.csv
    Final_Output/{session_id}_mitigation_report.json

This provides a complete audit trail per session without requiring DVC. 
Intermediate files are cleaned up post-run while final outputs are retained permanently.

### LOGGING & MONITORING
#### Logging includes:
Job submission per user
Dataflow completion status
File combination results
Bias metric generation
Error handling with retries
Failures update Firestore session status to error

### ERROR HANDLING
#### Handled scenarios:
No active users
Dataflow job failures
Missing GCS files
Schema violations
Empty outputs
Retries implemented for GCS operations with exponential backoff


### UNIT TESTING
#### Framework: pytest
#### Test coverage:
Preprocessing transforms
Ratio calculations
Null handling
Bias metric computation
GCS combine logic (mocked)


### PERFORMANCE OPTIMIZATION AND COST SAVING
Parallel Dataflow jobs per user
GCS file combination with streaming logic
Minimal in-memory aggregation
Retry logic for transient failures
Usage of Cloud Run + Cloud Function + Cloud Pub/Sub over Cloud Composer for lower resource costs

### FUTURE UPDATES
Reduction of cold startup time
Usage of BigQuery for storage of analytical data
End batch pipeline with calling of ML model and handing data directly over to it


