# Spark Observability Profiler

This script analyzes EMR and Databricks Spark cluster performance by extracting metrics from the Spark History Server. It automatically discovers clusters (or uses a provided ARN), connects to their history UIs, collects application, job, stage, and query details, and generates Spark DataFrames (or writes to S3) for performance analysis and optimization.

### Key Features
- Discovers EMR or Databricks clusters and reads Spark history data.
- Extracts and processes application, job, stage, and SQL query metrics.
- Produces Spark DataFrames for further analysis and optional S3 output.
- Designed for scalable, multi-cluster profiling to aid performance tuning.

## AWS IAM Permissions
*   **elasticmapreduce**:
    *   `ListClusters`
    *   `DescribeCluster`
    *   `ListSteps`
    *   `DescribeStep`
    *   `CreatePersistentAppUI`
    *   `GetPersistentAppUIPresignedURL`
    *   `ListInstanceGroups`
    *   `ListInstanceFleets`
*   **s3**:
    *   `PutObject`
    *   `GetObject`
    *   `ListBucket`
*   **sts**:
    *   `GetCallerIdentity`

## Databricks Permissions

i) Create a secret for token and then fetch token in code (in this example we leverage dbutils with secret scope shscreds but you can name these whatever)

ii) Create a secret for data plane URL and then fetch token in code. You can find the data plane URL by navigating to the spark UI for any completed job, clicking 'open in new tab' and then copying the URL in the top navbar (should contain dp-)

iii) Create a secret for DATAPLANE_DOMAIN_DBAUTH and then fetch token in code. You can find the DATAPLANE_DOMAIN_DBAUTH cookie by navigating to the spark UI for any completed job, clicking 'open in new tab' and then copying the DATAPLANE_DOMAIN_DBAUTH cookie that you see when opening the 'inspect' devtools and navigating to application tab. 

## Configurable Environment Variables
- **`aws_region`**: AWS region (e.g., `us-east-1`).
- **`emr_cluster_arn`**: Specify a cluster ARN, or leave blank to auto-discover.
- **`timeout_seconds`**: Timeout for requests (default: `300`).
- **`max_applications`**: Maximum number of applications to analyze per cluster.
- **`environment`**: Set to `dev` or `prod`.
- **`s3_output_path`**: S3 path for results (required in `prod` environment).
- **`cluster_states`**: Cluster states to analyze (e.g., `TERMINATED`, `WAITING`, both, or `ALL`).
- **`cluster_name_filter`**: Optional substring filter for cluster names.
- **`max_clusters`**: Maximum number of clusters to analyze (default: `5`).
- **`created_after_date` / `created_before_date`**: Optional date range filter (format: `YYYY-MM-DD`).
- **`persistent_ui_timeout_seconds`**: Timeout for the persistent UI to become ready.
- **`max_cluster_threads`**: Maximum number of concurrent clusters to analyze.
- **`max_app_threads`**: Maximum number of concurrent application analyses per cluster.


