# Databricks notebook source
# DBTITLE 1,Install Boto3
#Run cell on cluster restart or if receiving error:
#AttributeError: 'EMR' object has no attribute 'create_persistent_app_ui'
%pip install --upgrade boto3 botocore
%restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## EMR Spark Event Log Analyzer
# MAGIC
# MAGIC This script analyzes EMR clusters to extract performance metrics from Spark History Servers. It discovers EMR clusters, connects to their persistent Spark History Server UIs, fetches application, job, stage, and SQL query data, and then processes this information into Spark DataFrames for performance analysis and optimization insights.
# MAGIC
# MAGIC ## Required IAM Permissions
# MAGIC #### EMR
# MAGIC - elasticmapreduce:ListClusters
# MAGIC - elasticmapreduce:DescribeCluster
# MAGIC - elasticmapreduce:ListSteps
# MAGIC - elasticmapreduce:DescribeStep
# MAGIC - elasticmapreduce:CreatePersistentAppUI
# MAGIC - elasticmapreduce:GetPersistentAppUIPresignedURL
# MAGIC - elasticmapreduce:ListInstanceGroups
# MAGIC - elasticmapreduce:ListInstanceFleets
# MAGIC #### S3
# MAGIC - s3:PutObject
# MAGIC - s3:GetObject
# MAGIC - s3:ListBucket
# MAGIC #### STS
# MAGIC - sts:GetCallerIdentity

# COMMAND ----------

# DBTITLE 1,Config
import datetime
from datetime import timedelta, date
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("databricks").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Configuration Parameters
# ----------------------------------------------------------------------
# Parse and validate configuration
dbutils.widgets.text("aws_region", "us-east-1", "AWS Region")
dbutils.widgets.text("emr_cluster_arn", "", "EMR Cluster ARN (optional - leave blank to discover clusters)")
dbutils.widgets.text("timeout_seconds", "300", "Request Timeout (seconds)")
dbutils.widgets.text("max_applications", "10", "Max Applications to Analyze per Cluster")
dbutils.widgets.dropdown("environment", "dev", ["dev", "prod"], "Environment (dev/prod)")
dbutils.widgets.text("s3_output_path", "", "S3 Output Path (prod only)")
dbutils.widgets.text("custom_hours_threshold", "", "Normalized instance hours threshold (optional)")
dbutils.widgets.dropdown("cluster_states", "TERMINATED,WAITING", ["TERMINATED", "WAITING", "TERMINATED,WAITING", "ALL"], "EMR Cluster States to Analyze")
dbutils.widgets.text("cluster_name_filter", "", "Cluster Name Filter (optional - partial name match)")
dbutils.widgets.text("max_clusters", "50", "Max Clusters to Analyze")
dbutils.widgets.text("created_after_date", "", "EMR Clusters Created After (YYYY-MM-DD)")
dbutils.widgets.text("created_before_date", "", "EMR Clusters Created Before (YYYY-MM-DD)")
dbutils.widgets.text("persistent_ui_timeout_seconds", "180", "Persistent App UI Timeout (seconds)")
dbutils.widgets.text("batch_size", "50", "Batch Size (clusters to process concurrently)")
dbutils.widgets.text("batch_delay_seconds", "1800", "Delay Between Batches (seconds)")
dbutils.widgets.text("max_endpoint_failures", "3", "Max Endpoint Failures per Endpoint Type")

# Retrieve and parse configuration values
CUSTOM_HOURS_THRESHOLD = int(dbutils.widgets.get("custom_hours_threshold").strip() or 0)
if CUSTOM_HOURS_THRESHOLD > 0:
    logger.info("Custom normalized instance hours threshold set to: %s", CUSTOM_HOURS_THRESHOLD)

AWS_REGION = dbutils.widgets.get("aws_region").strip() or "us-east-1"
EMR_CLUSTER_ARN = dbutils.widgets.get("emr_cluster_arn").strip()
TIMEOUT_SECONDS = int(dbutils.widgets.get("timeout_seconds") or "300")
MAX_APPLICATIONS = int(dbutils.widgets.get("max_applications") or "50")
CLUSTER_STATES = dbutils.widgets.get("cluster_states").strip()
CLUSTER_NAME_FILTER = dbutils.widgets.get("cluster_name_filter").strip()
MAX_CLUSTERS = int(dbutils.widgets.get("max_clusters") or "5")
ENVIRONMENT = dbutils.widgets.get("environment").strip()
S3_OUTPUT_PATH = dbutils.widgets.get("s3_output_path").strip()
PERSISTENT_UI_TIMEOUT_SECONDS = int(dbutils.widgets.get("persistent_ui_timeout_seconds") or "180")
BATCH_SIZE = int(dbutils.widgets.get("batch_size") or "3")
BATCH_DELAY_SECONDS = int(dbutils.widgets.get("batch_delay_seconds") or "60")
MAX_ENDPOINT_FAILURES = int(dbutils.widgets.get("max_endpoint_failures") or "3")

# Date parameters handling
CREATED_AFTER_DATE_STR = dbutils.widgets.get("created_after_date").strip()
CREATED_BEFORE_DATE_STR = dbutils.widgets.get("created_before_date").strip()

PARSED_CREATED_AFTER_DATE = None
if CREATED_AFTER_DATE_STR:
    try:
        PARSED_CREATED_AFTER_DATE = datetime.datetime.strptime(CREATED_AFTER_DATE_STR, '%Y-%m-%d')
    except ValueError as e:
        logger.error("Invalid format for created_after_date: %s. Expected YYYY-MM-DD.", CREATED_AFTER_DATE_STR, exc_info=True)
        raise ValueError(f"Invalid format for created_after_date: {CREATED_AFTER_DATE_STR}. Expected YYYY-MM-DD.") from e

PARSED_CREATED_BEFORE_DATE = None
if CREATED_BEFORE_DATE_STR:
    try:
        PARSED_CREATED_BEFORE_DATE = datetime.datetime.strptime(CREATED_BEFORE_DATE_STR, '%Y-%m-%d') + timedelta(days=1, seconds=-1)
    except ValueError as e:
        logger.error("Invalid format for created_before_date: %s. Expected YYYY-MM-DD.", CREATED_BEFORE_DATE_STR, exc_info=True)
        raise ValueError(f"Invalid format for created_before_date: {CREATED_BEFORE_DATE_STR}. Expected YYYY-MM-DD.") from e

if PARSED_CREATED_AFTER_DATE and PARSED_CREATED_BEFORE_DATE and PARSED_CREATED_AFTER_DATE >= PARSED_CREATED_BEFORE_DATE:
    logger.error("created_after_date (%s) cannot be on or after created_before_date (%s).", CREATED_AFTER_DATE_STR, CREATED_BEFORE_DATE_STR, exc_info=True)
    raise ValueError("created_after_date cannot be on or after created_before_date.")

# Parse cluster states
if CLUSTER_STATES == "ALL":
    CLUSTER_STATES_LIST = ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING', 'TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS']
elif CLUSTER_STATES == "TERMINATED":
    CLUSTER_STATES_LIST = ['TERMINATED']
elif CLUSTER_STATES == "WAITING":
    CLUSTER_STATES_LIST = ['WAITING']
elif CLUSTER_STATES == "TERMINATED,WAITING":
    CLUSTER_STATES_LIST = ['TERMINATED', 'WAITING']
else:
    CLUSTER_STATES_LIST = ['TERMINATED', 'WAITING']
    logger.warning("Invalid cluster_states value '%s'. Defaulting to ['TERMINATED', 'WAITING'].", CLUSTER_STATES)

# Output versioning in prod
TODAY_STR = date.today().isoformat()
OUTPUT_RUN_PATH = f"{S3_OUTPUT_PATH.rstrip('/')}/{TODAY_STR}" if ENVIRONMENT == "prod" and S3_OUTPUT_PATH else S3_OUTPUT_PATH

# SHS Retry Configuration
SHS_RETRY_CONFIG = {
    "retry_settings": {
        "max_retries": 3,
        "initial_delay_seconds": 5.0,
        "max_delay_seconds": 120.0,
        "backoff_factor": 2.0,
        "enable_jitter": True,
        "jitter_max": 1.0
    },
    "timeout_settings": {
        "connection_timeout": 30,
        "read_timeout": 180,
        "total_timeout": 300
    },
    "throttling_settings": {
        "request_delay_seconds": 1.0,
        "enable_adaptive_throttling": True,
        "consecutive_error_threshold": 5,
        "adaptive_delay_multiplier": 1.5
    },
    "spark_history_server_errors": {
        "retryable_http_codes": [429, 500, 502, 503, 504],
        "non_retryable_codes": [400, 401, 403, 404],
        "connection_errors": ["ConnectionError", "Timeout", "HTTPError"]
    },
    "endpoint_specific_settings": {
        "stages": {
            "max_retries": 2,
            "read_timeout": 600
        },
        "jobs": {
            "max_retries": 2,
            "read_timeout": 600
        },
        "sql": {
            "max_retries": 2,
            "read_timeout": 600
        },
        "executors": {
            "max_retries": 2,
            "read_timeout": 300
        },
        "applications": {
            "max_retries": 2,
            "read_timeout": 300
        }
    },
    "logging": {
        "enable_retry_logging": True,
        "enable_success_logging": True,
        "enable_throttle_logging": True,
        "log_shs_url": True
    }
}

# Log final configuration
print("Configuration:")
print(f" Environment: {ENVIRONMENT}")
print(f" AWS Region: {AWS_REGION}")
print(f" EMR Cluster ARN: {EMR_CLUSTER_ARN or 'Auto-discover clusters'}")
print(f" Timeout: {TIMEOUT_SECONDS} seconds")
print(f" Max Applications per Cluster: {MAX_APPLICATIONS}")
print(f" Cluster States to Analyze: {CLUSTER_STATES_LIST}")
print(f" Cluster Name Filter: {CLUSTER_NAME_FILTER or 'None'}")
print(f" Max Clusters to Analyze: {MAX_CLUSTERS}")
print(f" Created After Date: {CREATED_AFTER_DATE_STR or 'None'}")
print(f" Created Before Date: {CREATED_BEFORE_DATE_STR or 'None'}")
print(f" S3 Output Path: {S3_OUTPUT_PATH or 'None (dev mode or not specified)'}")
print(f" Output Run Path: {OUTPUT_RUN_PATH or 'None'}")
print(f" Persistent UI Timeout: {PERSISTENT_UI_TIMEOUT_SECONDS} seconds")
print(f" Batch Size: {BATCH_SIZE} clusters")
print(f" Batch Delay: {BATCH_DELAY_SECONDS} seconds")
print(f" Max Endpoint Failures: {MAX_ENDPOINT_FAILURES}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### AWS Boto3 Helper Functions

# COMMAND ----------

# DBTITLE 1,Boto3 Helper Functions
"""
EMR Persistent App UI Client

This module provides functionality to create an EMR Persistent App UI, retrieve its details and presigned URL,
and establish an HTTP session with proper cookie management for Spark History Server access.
"""
import logging
import time
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

import boto3
import requests
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ServerConfig:
    """Configuration class for EMR Persistent UI client."""
    def __init__(self, emr_cluster_arn: str, timeout: int = 300):
        """
        Initialize ServerConfig.

        :param emr_cluster_arn: The EMR cluster ARN.
        :param timeout: Request timeout in seconds.
        :raises ValueError: If emr_cluster_arn is invalid.
        """
        if not emr_cluster_arn or not emr_cluster_arn.startswith("arn:aws:elasticmapreduce:"):
            raise ValueError("Invalid EMR cluster ARN format")
        self.emr_cluster_arn = emr_cluster_arn
        self.timeout = timeout


class EMRPersistentUIClient:
    """Client for managing EMR Persistent App UI and HTTP sessions."""
    def __init__(self, server_config: ServerConfig):
        """
        Initialize the EMR client.

        :param server_config: ServerConfig object containing cluster ARN and timeout.
        """
        self.emr_cluster_arn = server_config.emr_cluster_arn
        self.region = self.emr_cluster_arn.split(":")[3]
        self.emr_client = boto3.client("emr", region_name=self.region)
        self.session = requests.Session()
        self.persistent_ui_id: Optional[str] = None
        self.presigned_url: Optional[str] = None
        self.base_url: Optional[str] = None
        self.timeout: int = server_config.timeout
        self.presigned_url_ready: bool = False

    def create_persistent_app_ui(self) -> Dict:
        """
        Create a persistent app UI for the given cluster.

        :returns: Response from create-persistent-app-ui API call.
        :raises ClientError: If the API call fails.
        """
        logger.info("Creating persistent app UI for cluster: %s", self.emr_cluster_arn)
        try:
            response = self.emr_client.create_persistent_app_ui(
                TargetResourceArn=self.emr_cluster_arn
            )
            self.persistent_ui_id = response.get("PersistentAppUIId")
            logger.info("✅ Persistent App UI created successfully with ID: %s", self.persistent_ui_id)
            return response
        except ClientError as e:
            logger.error("❌ Failed to create persistent app UI: %s", e.response["Error"]["Message"], exc_info=True)
            raise

    def get_presigned_url(self, ui_type: str = "SHS") -> str:
        """
        Get presigned URL for the persistent app UI.

        :param ui_type: Type of UI ('SHS' for Spark History Server).
        :returns: Presigned URL string.
        :raises ValueError: If no persistent UI ID is available.
        :raises ClientError: If the API call fails.
        """
        if not self.persistent_ui_id:
            raise ValueError("No persistent UI ID available. Create one first.")
        logger.info("Getting presigned URL for persistent app UI: %s (type: %s)", self.persistent_ui_id, ui_type)
        try:
            response = self.emr_client.get_persistent_app_ui_presigned_url(
                PersistentAppUIId=self.persistent_ui_id,
                PersistentAppUIType=ui_type
            )
            self.presigned_url = response.get("PresignedURL")
            parsed_url = urlparse(self.presigned_url)
            self.base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/shs"
            logger.info("✅ Presigned URL obtained successfully. Base URL: %s", self.base_url)
            return self.presigned_url
        except ClientError as e:
            logger.error("❌ Failed to get presigned URL: %s", e.response["Error"]["Message"], exc_info=True)
            raise

    def setup_http_session(self) -> requests.Session:
        """
        Set up HTTP session with proper headers and cookie management.

        :returns: Configured requests.Session object.
        :raises ValueError: If no presigned URL is available.
        :raises requests.exceptions.RequestException: If session setup fails.
        """
        if not self.presigned_url:
            raise ValueError("No presigned URL available. Get one first.")
        logger.info("Setting up HTTP session with cookie management")
        self.session.headers.update({
            "User-Agent": "EMR-Persistent-UI-Client/1.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        })
        try:
            response = self.session.get(self.presigned_url, timeout=self.timeout, allow_redirects=True)
            response.raise_for_status()
            logger.info("✅ HTTP session established successfully (Status: %s)", response.status_code)
            return self.session
        except requests.exceptions.RequestException as e:
            logger.error("❌ Failed to establish HTTP session: %s", str(e), exc_info=True)
            raise

    def initialize(self, max_wait_time: int) -> Tuple[str, requests.Session]:
        """
        Initialize the EMR Persistent UI client by creating a persistent app UI,
        polling until it is ready, getting a presigned URL, and setting up an HTTP session.

        :param max_wait_time: The maximum time in seconds to wait for the persistent UI to become ready.
        :returns: Tuple containing the base URL and configured session.
        :raises ValueError: If the persistent UI does not become ready within the timeout period.
        """
        self.create_persistent_app_ui()

        wait_interval = 10
        total_waited = 0
        url_is_ready = False
        logger.info("Waiting for Persistent App UI to become ready...")
        while total_waited < max_wait_time:
            try:
                response = self.emr_client.get_persistent_app_ui_presigned_url(
                    PersistentAppUIId=self.persistent_ui_id, PersistentAppUIType="SHS"
                )
                url_is_ready = response.get("PresignedURLReady", False)
            except ClientError as e:
                logger.warning("Could not check for presigned URL, will retry. Error: %s", str(e))

            if url_is_ready:
                logger.info("✅ Persistent App UI is ready.")
                break

            logger.info("Persistent App UI not ready yet. Waiting %s seconds before retrying...", wait_interval)
            time.sleep(wait_interval)
            total_waited += wait_interval

        if not url_is_ready:
            raise ValueError(f"Persistent App UI did not become ready after waiting {total_waited} seconds.")

        self.get_presigned_url()
        self.setup_http_session()

        if not self.base_url or not self.session:
            raise RuntimeError("Initialization failed to produce a valid base URL and session.")

        return self.base_url, self.session


# COMMAND ----------

# MAGIC %md
# MAGIC ### EMR Cluster Discovery

# COMMAND ----------

# DBTITLE 1,EMR Cluster Discovery
from datetime import datetime
from typing import List, Dict, Optional
from botocore.exceptions import ClientError

class EMRClusterDiscovery:
    """Discovery and management of EMR clusters."""

    def __init__(self, region: str):
        """
        Initialize the EMR cluster discovery client.

        :param region: AWS region.
        :raises TypeError: If region is not a non-empty string.
        """
        if not isinstance(region, str) or not region:
            raise TypeError("AWS region must be a non-empty string.")
        self.region = region
        self.emr_client = boto3.client("emr", region_name=region)

    def discover_clusters(
        self,
        states: Optional[List[str]] = None,
        name_filter: Optional[str] = None,
        max_clusters: int = 10,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
        custom_hours_threshold: int = 0) -> List[Dict]:
        """
        Discover EMR clusters based on criteria.
        """
        if not isinstance(states, (list, type(None))):
            raise TypeError(":param states: must be a list of strings or None.")
        if not isinstance(max_clusters, int) or max_clusters < 1:
            raise ValueError(":param max_clusters: must be an integer >= 1.")

        logger.info("🔍 Discovering EMR clusters in region: %s", self.region)
        try:
            list_clusters_params = {}
            list_clusters_params['ClusterStates'] = states or ['TERMINATED', 'WAITING']
            if created_after:
                list_clusters_params['CreatedAfter'] = created_after
            if created_before:
                list_clusters_params['CreatedBefore'] = created_before

            paginator = self.emr_client.get_paginator('list_clusters')
            page_iterator = paginator.paginate(**list_clusters_params)

            discovered_clusters = []
            for page in page_iterator:
                for cluster in page.get('Clusters', []):
                    if name_filter and name_filter.lower() not in cluster.get('Name', '').lower():
                        continue
                    if custom_hours_threshold > 0 and cluster.get('NormalizedInstanceHours', 0) < custom_hours_threshold:
                        continue

                    cluster_info = {
                        'cluster_id': cluster.get('Id'),
                        'cluster_name': cluster.get('Name'),
                        'cluster_arn': cluster.get('ClusterArn'),
                        'status': cluster.get('Status', {}).get('State'),
                        'creation_time': cluster.get('Status', {}).get('Timeline', {}).get('CreationDateTime'),
                        'normalized_instance_hours': cluster.get('NormalizedInstanceHours', 0)
                    }
                    discovered_clusters.append(cluster_info)

                    if len(discovered_clusters) >= max_clusters:
                        break
                if len(discovered_clusters) >= max_clusters:
                    break

            logger.info("✅ Discovered %s clusters", len(discovered_clusters))
            return discovered_clusters
        except ClientError as e:
            logger.error("❌ Failed to discover clusters: %s", e.response["Error"]["Message"], exc_info=True)
            raise

    def get_cluster_details(self, cluster_id: str) -> Dict:
        """
        Get detailed information about a specific cluster, with instance groups/fleets fallback.
        """
        if not isinstance(cluster_id, str) or not cluster_id:
            raise TypeError("Cluster ID must be a non-empty string.")

        try:
            response = self.emr_client.describe_cluster(ClusterId=cluster_id)
            cluster = response.get('Cluster', {})

            details = {
                'cluster_id': cluster.get('Id'),
                'cluster_name': cluster.get('Name'),
                'cluster_arn': cluster.get('ClusterArn'),
                'status': cluster.get('Status', {}).get('State'),
                'applications': [app.get('Name') for app in cluster.get('Applications', [])],
                'normalized_instance_hours': cluster.get('NormalizedInstanceHours', 0)
            }

            # Instance metadata: try InstanceGroups, fall back to InstanceFleets
            instance_metadata = []
            collection_type = None

            try:
                ig_resp = self.emr_client.list_instance_groups(ClusterId=cluster_id)
                groups = ig_resp.get('InstanceGroups', [])
                if groups:
                    collection_type = 'INSTANCE_GROUPS'
                    for g in groups:
                        instance_metadata.append({
                            'collection': 'GROUP',
                            'role_or_type': g.get('InstanceGroupType'),
                            'instance_type': g.get('InstanceType'),
                            'requested_instances': g.get('RequestedInstanceCount'),
                            'running_instances': g.get('RunningInstanceCount'),
                            'market': g.get('Market')
                        })
            except ClientError as e:
                err = getattr(e, "response", {}).get("Error", {}) if hasattr(e, "response") else {}
                code = err.get("Code", "")
                msg = err.get("Message", "") or str(e)

                # Suppress the noisy, expected case for fleets; keep other failures as warnings
                if code == "InvalidRequestException" and "Instance fleets and instance groups are mutually exclusive" in msg:
                    logger.debug("list_instance_groups skipped for %s (cluster uses Instance Fleets)", cluster_id)
                else:
                    logger.warning("list_instance_groups failed for %s: %s", cluster_id, msg)

            if not instance_metadata:
                try:
                    if_resp = self.emr_client.list_instance_fleets(ClusterId=cluster_id)
                    fleets = if_resp.get('InstanceFleets', [])
                    if fleets:
                        collection_type = 'INSTANCE_FLEETS'
                        for f in fleets:
                            instance_types = [it.get('InstanceType') for it in f.get('InstanceTypeSpecifications', [])]
                            instance_metadata.append({
                                'collection': 'FLEET',
                                'role_or_type': f.get('InstanceFleetType'),
                                'instance_types': instance_types,
                                'target_on_demand': f.get('TargetOnDemandCapacity'),
                                'target_spot': f.get('TargetSpotCapacity'),
                                'status': f.get('Status', {}).get('State')
                            })
                except ClientError as e:
                    logger.warning("list_instance_fleets failed for %s: %s", cluster_id, str(e))

            if instance_metadata:
                details['instance_collection_type'] = collection_type
                details['instance_metadata'] = instance_metadata

            return details

        except ClientError as e:
            logger.error("❌ Failed to describe cluster %s: %s", cluster_id, e.response["Error"]["Message"], exc_info=True)
            raise

    def validate_cluster_for_analysis(self, cluster_info: Dict) -> bool:
        """
        Validate if a cluster is suitable for Spark History Server analysis.
        """
        if not isinstance(cluster_info, dict):
            raise TypeError(":param cluster_info: must be a dictionary.")
        has_spark = any('Spark' in app for app in cluster_info.get('applications', []))
        if not has_spark:
            logger.warning("⚠️ Cluster %s does not have Spark installed - skipping", cluster_info.get('cluster_id'))
            return False
        valid_states = ['RUNNING', 'WAITING', 'TERMINATED']
        if cluster_info.get('status') not in valid_states:
            logger.warning("⚠️ Cluster %s is in state '%s' - may not have history data.", cluster_info.get('cluster_id'), cluster_info.get('status'))
            return False
        logger.info("✅ Cluster %s is valid for analysis.", cluster_info.get('cluster_id'))
        return True


# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark History Server REST Interaction

# COMMAND ----------

# DBTITLE 1,SHS REST Interactions
import json
import random
import time
from typing import List, Any, Dict, Optional
import requests
import logging

logger = logging.getLogger(__name__)

class SparkHistoryServerClient:
    """Client for interacting with Spark History Server REST API."""

    def __init__(self, base_url: str, session: requests.Session):
        """
        Initialize the Spark History Server client.

        :param base_url: Base URL for the Spark History Server.
        :param session: Configured HTTP session with authentication.
        """
        if not base_url or not isinstance(base_url, str):
            raise ValueError("base_url must be a non-empty string.")
        if not isinstance(session, requests.Session):
            raise TypeError("session must be a requests.Session object.")
            
        self.base_url = base_url
        self.session = session
        self.api_base = f"{base_url}/api/v1"
        self.retry_config = SHS_RETRY_CONFIG
        self.consecutive_errors = {}  # Track consecutive errors per endpoint
        self.last_request_time = 0  # For throttling

    def _calculate_jitter(self, delay: float) -> float:
        """Calculate jitter for backoff delay."""
        if not self.retry_config["retry_settings"]["enable_jitter"]:
            return delay
        
        jitter_max = self.retry_config["retry_settings"]["jitter_max"]
        jitter = random.uniform(0, jitter_max)
        return delay + jitter

    def _get_endpoint_type(self, endpoint: str) -> str:
        """Extract endpoint type from endpoint path for configuration lookup."""
        if '/sql' in endpoint:
            return 'sql'
        elif '/stages' in endpoint:
            return 'stages'
        elif '/jobs' in endpoint:
            return 'jobs'
        elif '/allexecutors' in endpoint:
            return 'executors'
        elif 'applications' in endpoint and not any(x in endpoint for x in ['/jobs', '/stages', '/sql', '/allexecutors']):
            return 'applications'
        else:
            return 'default'

    def _apply_throttling(self, endpoint_type: str):
        """Apply throttling based on configuration and consecutive errors."""
        config = self.retry_config
        throttling = config["throttling_settings"]
        
        # Base request delay
        base_delay = throttling["request_delay_seconds"]
        
        # Adaptive throttling based on consecutive errors
        if throttling["enable_adaptive_throttling"]:
            consecutive_errors = self.consecutive_errors.get(endpoint_type, 0)
            if consecutive_errors >= throttling["consecutive_error_threshold"]:
                adaptive_delay = base_delay * (throttling["adaptive_delay_multiplier"] ** (consecutive_errors - throttling["consecutive_error_threshold"] + 1))
                base_delay = min(adaptive_delay, config["retry_settings"]["max_delay_seconds"])
                
                if config["logging"]["enable_throttle_logging"]:
                    logger.info("Applying adaptive throttling for %s: %d consecutive errors, delay: %.2fs", 
                              endpoint_type, consecutive_errors, base_delay)
        
        # Ensure minimum time between requests
        time_since_last = time.time() - self.last_request_time
        if time_since_last < base_delay:
            sleep_time = base_delay - time_since_last
            if config["logging"]["enable_throttle_logging"]:
                logger.info("Throttling request for %s: sleeping %.2fs", endpoint_type, sleep_time)
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()

    def _is_retryable_error(self, exception: Exception) -> bool:
        """Determine if an error is retryable based on configuration."""
        error_config = self.retry_config["spark_history_server_errors"]
        
        if isinstance(exception, requests.exceptions.HTTPError):
            status_code = exception.response.status_code
            if status_code in error_config["non_retryable_codes"]:
                return False
            return status_code in error_config["retryable_http_codes"]
        
        # Check connection errors
        exception_name = type(exception).__name__
        return exception_name in error_config["connection_errors"]

    def _make_request(self, endpoint: str, params: Optional[Dict] = None, max_retries: int = None) -> Any:
        """
        Make a REST API request with enhanced retry logic based on SHS_RETRY_CONFIG.

        :param endpoint: The API endpoint to call (e.g., 'applications/app-123/jobs').
        :param params: A dictionary of query parameters for the request.
        :param max_retries: Optional override for max retries (uses config if None).
        :returns: The JSON response from the API.
        :raises requests.exceptions.RequestException: If the request fails after all retries.
        """
        url = f"{self.api_base}/{endpoint}"
        endpoint_type = self._get_endpoint_type(endpoint)
        config = self.retry_config
        
        # Get endpoint-specific settings
        endpoint_settings = config["endpoint_specific_settings"].get(endpoint_type, {})
        
        # Determine max retries (endpoint-specific > parameter > global config)
        if max_retries is None:
            max_retries = endpoint_settings.get("max_retries", config["retry_settings"]["max_retries"])
        
        # Determine timeouts
        connection_timeout = config["timeout_settings"]["connection_timeout"]
        read_timeout = endpoint_settings.get("read_timeout", config["timeout_settings"]["read_timeout"])
        timeout = (connection_timeout, read_timeout)
        
        if config["logging"]["log_shs_url"] and config["logging"]["enable_retry_logging"]:
            logger.info("Making request to: %s (endpoint_type: %s, max_retries: %d)", url, endpoint_type, max_retries)
        
        # Apply throttling before making request
        self._apply_throttling(endpoint_type)
        
        last_exception = None
        
        # The retry loop starts with attempt 0 (the first try)
        for attempt in range(max_retries + 1):
            if attempt > 0:
                # Calculate backoff delay with jitter
                base_delay = config["retry_settings"]["initial_delay_seconds"] * (
                    config["retry_settings"]["backoff_factor"] ** (attempt - 1)
                )
                delay = min(base_delay, config["retry_settings"]["max_delay_seconds"])
                delay_with_jitter = self._calculate_jitter(delay)
                
                if config["logging"]["enable_retry_logging"]:
                    logger.info("Retrying %s in %.2f seconds (attempt %d/%d)...", 
                              endpoint, delay_with_jitter, attempt, max_retries)
                
                time.sleep(delay_with_jitter)

            try:
                response = self.session.get(url, params=params, timeout=timeout)
                response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
                
                # Reset consecutive errors on success
                self.consecutive_errors[endpoint_type] = 0
                
                if config["logging"]["enable_success_logging"]:
                    logger.debug("✅ Successfully retrieved data from %s (attempt %d)", endpoint, attempt + 1)
                
                return response.json()
                
            except Exception as e:
                last_exception = e
                
                # Track consecutive errors for adaptive throttling
                self.consecutive_errors[endpoint_type] = self.consecutive_errors.get(endpoint_type, 0) + 1
                
                # Check if error is retryable
                if not self._is_retryable_error(e):
                    if config["logging"]["enable_retry_logging"]:
                        logger.error("❌ Non-retryable error for %s: %s", url, str(e))
                    raise e
                
                if config["logging"]["enable_retry_logging"]:
                    logger.warning("Retryable error on attempt %d for %s: %s", attempt + 1, url, str(e))
                
                # If this was the last attempt, don't continue
                if attempt == max_retries:
                    break

        # All retries exhausted
        if config["logging"]["enable_retry_logging"]:
            logger.error("All %d retry attempts failed for %s.", max_retries + 1, url)
        
        raise last_exception

    def get_applications(self, status: Optional[str] = None, limit: int = 100) -> List[Dict]:
        logger.info("Fetching applications (status: %s, limit: %s)", status, limit)
        params = {'limit': limit}
        if status:
            params['status'] = status
        return self._make_request("applications", params)

    def get_application_details(self, app_id: str) -> Dict:
        logger.info("Fetching application details for: %s", app_id)
        return self._make_request(f"applications/{app_id}")

    def get_application_jobs(self, app_id: str, attempt_id: Optional[str] = None, status: Optional[str] = None) -> List[Dict]:
        endpoint = f"applications/{app_id}/{attempt_id}/jobs" if attempt_id else f"applications/{app_id}/jobs"
        params = {'status': status} if status else {}
        return self._make_request(endpoint, params)

    def get_application_stages(self, app_id: str, attempt_id: Optional[str] = None, status: Optional[str] = None) -> List[Dict]:
        endpoint = f"applications/{app_id}/{attempt_id}/stages" if attempt_id else f"applications/{app_id}/stages"
        params = {'details': 'true', 'withSummaries': 'true'}
        if status:
            params['status'] = status
        return self._make_request(endpoint, params)

    def get_stage_tasks(self, app_id: str, attempt_id: str, stage_id: int, stage_attempt: int = 0) -> List[Dict]:
        endpoint = f"applications/{app_id}/{attempt_id}/stages/{stage_id}/{stage_attempt}/taskList"
        return self._make_request(endpoint)

    def get_stage_task_summary(self, app_id: str, attempt_id: str, stage_id: int, stage_attempt: int = 0) -> Dict:
        endpoint = f"applications/{app_id}/{attempt_id}/stages/{stage_id}/{stage_attempt}/taskSummary"
        params = {'quantiles': "0.0,0.25,0.5,0.75,1.0"}
        return self._make_request(endpoint, params)

    def get_application_executors(self, app_id: str, attempt_id: Optional[str] = None) -> List[Dict]:
        endpoint = f"applications/{app_id}/{attempt_id}/allexecutors" if attempt_id else f"applications/{app_id}/allexecutors"
        return self._make_request(endpoint)

    def get_application_sql_queries(self, app_id: str, attempt_id: Optional[str] = None) -> List[Dict]:
        endpoint = f"applications/{app_id}/{attempt_id}/sql" if attempt_id else f"applications/{app_id}/sql"
        params = {'details': 'true', 'planDescription': 'true'}
        return self._make_request(endpoint, params)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Metrics Analysis Functions

# COMMAND ----------

# DBTITLE 1,SHS Metric Analysis
import json
import logging
from typing import List, Dict, Tuple, Optional, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType
)

logger = logging.getLogger(__name__)


class SparkMetricsAnalyzer:
    """Analyzer for Spark application metrics and performance data."""

    def __init__(self, spark: SparkSession):
        """
        Initialize the metrics analyzer.

        :param spark: Spark session instance for DataFrame creation.
        :raises TypeError: If spark is not a SparkSession instance.
        """
        if not isinstance(spark, SparkSession):
            raise TypeError("Parameter 'spark' must be a SparkSession instance")
        self.spark = spark

    def analyze_application_performance(self, app_data: Dict, latest_attempt_id: str) -> Dict:
        """
        Analyze performance metrics for a single application.

        :param app_data: Application data from Spark History Server.
        :param latest_attempt_id: The latest attempt ID to extract data from.
        :returns: Performance analysis results dictionary.
        """
        latest_attempt_info = next((a for a in app_data.get('attempts', []) if a.get('attemptId') == latest_attempt_id), {})
        source_data = latest_attempt_info or app_data
        duration_ms = source_data.get('duration', 0)

        return {
            'application_id': app_data.get('id'),
            'application_name': app_data.get('name', 'Unknown'),
            'duration_ms': duration_ms,
            'duration_minutes': round(duration_ms / 60000, 2),
            'start_time': source_data.get('startTime'),
            'end_time': source_data.get('endTime'),
            'spark_version': latest_attempt_info.get('appSparkVersion') or app_data.get('sparkVersion', 'Unknown'),
        }

    def analyze_job_performance(self, jobs: List[Dict]) -> List[Dict]:
        """
        Analyze performance metrics for jobs.

        :param jobs: List of job data dictionaries.
        :returns: List of job performance analysis dictionaries.
        """
        job_analysis = []
        for job in jobs:
            num_tasks = job.get('numTasks', 0)
            num_completed = job.get('numCompletedTasks', 0)
            analysis = {
                'job_id': job.get('jobId'),
                'job_name': job.get('name', 'Unknown'),
                'status': job.get('status', 'UNKNOWN'),
                'submission_time': job.get('submissionTime'),
                'completion_time': job.get('completionTime'),
                'num_tasks': num_tasks,
                'num_completed_tasks': num_completed,
                'num_failed_tasks': job.get('numFailedTasks', 0),
                'stage_ids': str(job.get('stageIds', [])),
                'task_success_rate': round((num_completed / num_tasks) * 100.0, 2) if num_tasks > 0 else 0.0,
            }
            job_analysis.append(analysis)
        return job_analysis

    def analyze_stage_performance(self, stages: List[Dict]) -> List[Dict]:
        """
        Analyze performance metrics for stages.

        :param stages: List of stage data dictionaries.
        :returns: List of stage performance analysis dictionaries.
        """
        stage_analysis = []
        for stage in stages:
            analysis = {
                'stage_id': stage.get('stageId'),
                'stage_name': stage.get('name') or 'Unknown',
                'status': stage.get('status') or 'UNKNOWN',
                'num_tasks': stage.get('numTasks') or 0,
                'num_active_tasks': stage.get('numActiveTasks') or 0,
                'num_complete_tasks': stage.get('numCompleteTasks') or 0,
                'num_failed_tasks': stage.get('numFailedTasks') or 0,
                'executor_run_time': stage.get('executorRunTime') or 0,
                'executor_cpu_time': stage.get('executorCpuTime') or 0,
                'submission_time': stage.get('submissionTime'),
                'first_task_launched_time': stage.get('firstTaskLaunchedTime'),
                'completion_time': stage.get('completionTime'),
                'input_bytes': stage.get('inputBytes') or 0,
                'output_bytes': stage.get('outputBytes') or 0,
                'shuffle_read_bytes': stage.get('shuffleReadBytes') or 0,
                'shuffle_write_bytes': stage.get('shuffleWriteBytes') or 0,
                'memory_bytes_spilled': stage.get('memoryBytesSpilled') or 0,
                'disk_bytes_spilled': stage.get('diskBytesSpilled') or 0,
                'task_completion_rate': 0.0,
                'avg_executor_run_time_per_task': 0.0,
                'total_data_processed_mb': 0.0,
                'shuffle_data_mb': 0.0,
                'cluster_id': '',  # Will be populated by caller
                'cluster_name': '',  # Will be populated by caller
                'application_id': ''  # Will be populated by caller
            }

            # Calculate efficiency metrics with safe division
            num_tasks = analysis['num_tasks']
            num_complete = analysis['num_complete_tasks']
            executor_run_time = analysis['executor_run_time']
            
            if num_tasks > 0:
                analysis['task_completion_rate'] = round((num_complete / num_tasks) * 100.0, 2)
                if executor_run_time > 0:
                    analysis['avg_executor_run_time_per_task'] = round(executor_run_time / num_tasks, 2)

            # Calculate data processing metrics (convert bytes to MB)
            input_bytes = analysis['input_bytes']
            output_bytes = analysis['output_bytes']
            shuffle_read_bytes = analysis['shuffle_read_bytes']
            shuffle_write_bytes = analysis['shuffle_write_bytes']
            
            analysis['total_data_processed_mb'] = round((input_bytes + output_bytes) / (1024 * 1024), 2)
            analysis['shuffle_data_mb'] = round((shuffle_read_bytes + shuffle_write_bytes) / (1024 * 1024), 2)

            stage_analysis.append(analysis)

        return stage_analysis

    def analyze_task_performance(self, tasks: List[Dict]) -> List[Dict]:
        """
        Analyze performance metrics for tasks.

        :param tasks: List of task data dictionaries.
        :returns: List of task performance analysis dictionaries.
        """
        task_analysis = []
        for task in tasks:
            analysis = {
                'task_id': task.get('taskId'),
                'index': task.get('index'),
                'attempt': task.get('attempt'),
                'launch_time': task.get('launchTime'),
                'duration': task.get('duration'),
                'executor_id': task.get('executorId'),
                'host': task.get('host'),
                'status': task.get('status'),
                'task_locality': task.get('taskLocality'),
                'speculative': task.get('speculative'),
                'stage_id': task.get('stage_id'),
                'stage_attempt_id': task.get('stage_attempt_id'),
            }
            task_analysis.append(analysis)
        return task_analysis

    def analyze_sql_queries(self, sql_queries: List[Dict]) -> List[Dict]:
        """
        Analyze SQL query metrics.

        :param sql_queries: List of SQL query data dictionaries.
        :returns: List of processed SQL query analysis dictionaries.
        """
        sql_analysis = []
        for query in sql_queries:
            analysis = {
                'sql_id': query.get("id"),
                'description': query.get("description", "N/A"),
                'status': query.get("status"),
                'duration_ms': query.get("duration", 0),
                'submission_time': query.get("submissionTime"),
                'sql_raw_json': json.dumps(query),
            }
            sql_analysis.append(analysis)
        return sql_analysis

    def analyze_executor_performance(self, executors: List[Dict]) -> List[Dict]:
        """
        Analyze performance metrics for executors.

        :param executors: List of executor data dictionaries from Spark History Server.
        :returns: List of executor performance analysis dictionaries.
        """
        executor_analysis = []
        for executor in executors:
            analysis = {
                'executor_id': executor.get('id'),
                'host_port': executor.get('hostPort'),
                'is_active': executor.get('isActive'),
                'rdd_blocks': executor.get('rddBlocks'),
                'memory_used': executor.get('memoryUsed'),
                'disk_used': executor.get('diskUsed'),
                'total_cores': executor.get('totalCores'),
                'max_tasks': executor.get('maxTasks'),
                'active_tasks': executor.get('activeTasks'),
                'failed_tasks': executor.get('failedTasks'),
                'completed_tasks': executor.get('completedTasks'),
                'total_tasks': executor.get('totalTasks'),
                'total_duration': executor.get('totalDuration'),
                'total_gc_time': executor.get('totalGCTime'),
                'total_input_bytes': executor.get('totalInputBytes'),
                'total_shuffle_read': executor.get('totalShuffleRead'),
                'total_shuffle_write': executor.get('totalShuffleWrite'),
                'is_blacklisted': executor.get('isBlacklisted', False),
                'max_memory': executor.get('maxMemory'),
                'add_time': executor.get('addTime'),
                'executor_logs': json.dumps(executor.get('executorLogs', {})),
            }
            executor_analysis.append(analysis)
        return executor_analysis

    def analyze_task_summaries(self, task_summaries: List[Dict]) -> List[Dict]:
        """
        Analyze task summary metrics.

        :param task_summaries: List of task summary data dictionaries.
        :returns: List of processed task summary analysis dictionaries.
        """
        analysis_list = []
        for summary in task_summaries:
            analysis = {
                'application_id': summary.get('application_id'),
                'stage_id': summary.get('stage_id'),
                'stage_attempt_id': summary.get('stage_attempt_id'),
                'raw_json': json.dumps(summary),
            }
            analysis_list.append(analysis)
        return analysis_list

    def create_dynamic_dataframes(
        self,
        applications_analysis: List[Dict],
        jobs_analysis: List[Dict],
        stages_analysis: List[Dict],
        tasks_analysis: List[Dict],
        sql_analysis: List[Dict],
        executors_analysis: List[Dict],
        task_summaries_analysis: List[Dict]
        ) -> Tuple[Optional[DataFrame], Optional[DataFrame], Optional[DataFrame], Optional[DataFrame], Optional[DataFrame], Optional[DataFrame], Optional[DataFrame]]:
        """
        Create Spark DataFrames from analysis results with explicit, well-defined schemas.
        This version includes the 'attempt_id' field to uniquely identify data from each application run.
        """
        def create_df(data: List[Dict], schema: StructType, name: str) -> Optional[DataFrame]:
            if not data:
                logger.info("No data provided for %s DataFrame.", name)
                return None
            try:
                df = self.spark.createDataFrame(data, schema=schema)
                logger.info("✅ Created %s DataFrame with %d rows.", name, df.count())
                return df
            except Exception as e:
                logger.error("❌ Failed to create %s DataFrame: %s", name, str(e), exc_info=True)
                return None

        # --- Updated Schemas with attempt_id ---
        applications_schema = StructType([
            StructField("cluster_id", StringType(), True), StructField("cluster_name", StringType(), True),
            StructField("application_id", StringType(), True), StructField("attempt_id", StringType(), True),
            StructField("application_name", StringType(), True), StructField("duration_ms", LongType(), True),
            StructField("duration_minutes", DoubleType(), True), StructField("start_time", StringType(), True),
            StructField("end_time", StringType(), True), StructField("spark_version", StringType(), True)
        ])
        jobs_schema = StructType([
            StructField("cluster_id", StringType(), True), StructField("cluster_name", StringType(), True),
            StructField("application_id", StringType(), True), StructField("attempt_id", StringType(), True),
            StructField("job_id", LongType(), True), StructField("job_name", StringType(), True),
            StructField("status", StringType(), True), StructField("submission_time", StringType(), True),
            StructField("completion_time", StringType(), True), StructField("num_tasks", LongType(), True),
            StructField("num_completed_tasks", LongType(), True), StructField("num_failed_tasks", LongType(), True),
            StructField("stage_ids", StringType(), True), StructField("task_success_rate", DoubleType(), True)
        ])
        stages_schema = StructType([
            StructField("cluster_id", StringType(), True), StructField("cluster_name", StringType(), True),
            StructField("application_id", StringType(), True), StructField("attempt_id", StringType(), True),
            StructField("stage_id", LongType(), True), StructField("stage_name", StringType(), True),
            StructField("status", StringType(), True), StructField("num_tasks", LongType(), True),
            StructField("num_active_tasks", LongType(), True), StructField("num_complete_tasks", LongType(), True),
            StructField("num_failed_tasks", LongType(), True), StructField("executor_run_time", LongType(), True),
            StructField("executor_cpu_time", LongType(), True), StructField("submission_time", StringType(), True),
            StructField("first_task_launched_time", StringType(), True), StructField("completion_time", StringType(), True),
            StructField("input_bytes", LongType(), True), StructField("output_bytes", LongType(), True),
            StructField("shuffle_read_bytes", LongType(), True), StructField("shuffle_write_bytes", LongType(), True),
            StructField("memory_bytes_spilled", LongType(), True), StructField("disk_bytes_spilled", LongType(), True),
            StructField("task_completion_rate", DoubleType(), True), StructField("avg_executor_run_time_per_task", DoubleType(), True),
            StructField("total_data_processed_mb", DoubleType(), True), StructField("shuffle_data_mb", DoubleType(), True)
        ])
        tasks_schema = StructType([
            StructField("cluster_id", StringType(), True), StructField("cluster_name", StringType(), True),
            StructField("application_id", StringType(), True), StructField("attempt_id", StringType(), True),
            StructField("stage_id", LongType(), True), StructField("stage_attempt_id", LongType(), True),
            StructField("task_id", LongType(), True), StructField("index", LongType(), True),
            StructField("attempt", LongType(), True), StructField("launch_time", StringType(), True),
            StructField("duration", LongType(), True), StructField("executor_id", StringType(), True),
            StructField("host", StringType(), True), StructField("status", StringType(), True),
            StructField("task_locality", StringType(), True), StructField("speculative", BooleanType(), True)
        ])
        sql_schema = StructType([
            StructField("cluster_id", StringType(), True), StructField("cluster_name", StringType(), True),
            StructField("application_id", StringType(), True), StructField("attempt_id", StringType(), True),
            StructField("sql_id", LongType(), True), StructField("description", StringType(), True),
            StructField("status", StringType(), True), StructField("duration_ms", LongType(), True),
            StructField("submission_time", StringType(), True), StructField("sql_raw_json", StringType(), True)
        ])
        executors_schema = StructType([
            StructField("cluster_id", StringType(), True), StructField("cluster_name", StringType(), True),
            StructField("application_id", StringType(), True), StructField("attempt_id", StringType(), True),
            StructField("executor_id", StringType(), True), StructField("host_port", StringType(), True),
            StructField("is_active", BooleanType(), True), StructField("rdd_blocks", LongType(), True),
            StructField("memory_used", LongType(), True), StructField("disk_used", LongType(), True),
            StructField("total_cores", LongType(), True), StructField("max_tasks", LongType(), True),
            StructField("active_tasks", LongType(), True), StructField("failed_tasks", LongType(), True),
            StructField("completed_tasks", LongType(), True), StructField("total_tasks", LongType(), True),
            StructField("total_duration", LongType(), True), StructField("total_gc_time", LongType(), True),
            StructField("total_input_bytes", LongType(), True), StructField("total_shuffle_read", LongType(), True),
            StructField("total_shuffle_write", LongType(), True), StructField("is_blacklisted", BooleanType(), True),
            StructField("max_memory", LongType(), True), StructField("add_time", StringType(), True),
            StructField("executor_logs", StringType(), True)
        ])
        task_summaries_schema = StructType([
            StructField("cluster_id", StringType(), True), StructField("cluster_name", StringType(), True),
            StructField("application_id", StringType(), True), StructField("attempt_id", StringType(), True),
            StructField("stage_id", LongType(), True), StructField("stage_attempt_id", LongType(), True),
            StructField("raw_json", StringType(), True)
        ])
        
        # Create DataFrames
        apps_df = create_df(applications_analysis, applications_schema, "applications")
        jobs_df = create_df(jobs_analysis, jobs_schema, "jobs")
        stages_df = create_df(stages_analysis, stages_schema, "stages")
        tasks_df = create_df(tasks_analysis, tasks_schema, "tasks")
        sql_df = create_df(sql_analysis, sql_schema, "sql")
        executors_df = create_df(executors_analysis, executors_schema, "executors")
        task_summaries_df = create_df(task_summaries_analysis, task_summaries_schema, "task_summaries")

        return apps_df, jobs_df, stages_df, tasks_df, sql_df, executors_df, task_summaries_df



# COMMAND ----------

# MAGIC %md
# MAGIC ### Cluster Analyzer

# COMMAND ----------

# DBTITLE 1,Main Orchestration
import concurrent.futures
import json
import logging
from typing import List, Dict, Any, Optional
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# A private constant to limit the number of errors recorded per endpoint to avoid excessive memory usage.
_MAX_ERRORS_PER_ENDPOINT = 3

def _truncate_message(message: str, max_len: int = 500) -> str:
    """Truncates a message to a maximum length."""
    return message if len(message) <= max_len else message[:max_len-3] + "..."

def _record_error(endpoint: str, fn: Any, exc: Exception, endpoint_errors: Dict[str, List]):
    """Records a structured error for a failed endpoint call."""
    now_iso = datetime.now().isoformat()
    
    # Determine a short code for the error type
    if isinstance(exc, (requests.exceptions.ReadTimeout, requests.exceptions.Timeout)):
        code = "TIMEOUT"
    elif isinstance(exc, requests.exceptions.ConnectionError):
        code = "CONNECTION_ERROR"
    elif isinstance(exc, requests.exceptions.HTTPError):
        code = f"HTTP_{exc.response.status_code}"
    else:
        code = "UNKNOWN_ERROR"

    # Defensive: avoid masking original error recording if response properties fail
    error_obj = {
        'code': code,
        'exception_type': type(exc).__name__,
        'message': _truncate_message(str(exc)),
        'api': getattr(fn, '__name__', 'unknown'),
        'endpoint_key': endpoint,
        'timestamp': now_iso
    }

    # Append with cap
    lst = endpoint_errors.setdefault(endpoint, [])
    if len(lst) < _MAX_ERRORS_PER_ENDPOINT:
        lst.append(error_obj)

def process_single_application(
    app_id: str,
    attempt_id: str,
    shs_client: Any,
    analyzer: Any,
    cluster_id: str,
    cluster_name: str,
    endpoint_errors: Dict[str, List[Dict[str, Any]]],
    endpoint_attempted: Dict[str, bool],
    endpoint_skipped_reasons: Dict[str, List[str]], # Kept for consistent function signature
    app_details: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Processes a single Spark application attempt by fetching its data concurrently.
    This function uses a ThreadPoolExecutor to make parallel API calls for jobs, stages,
    executors, and SQL queries to significantly speed up data retrieval.
    """
    logger.info(" -> Concurrently analyzing application %s, attempt %s", app_id, attempt_id)
    app_results = {
        'applications': [], 'jobs': [], 'stages': [], 'tasks': [],
        'sql_queries': [], 'executors': [], 'task_summaries': []
    }

    def add_and_append(data_list: List[Dict], result_key: str):
        if not isinstance(data_list, list):
            logger.warning("⚠️ Data provided to add_and_append for key '%s' is not a list, skipping.", result_key)
            return
        for item in data_list:
            item['application_id'] = app_id
            item['attempt_id'] = attempt_id
            item['cluster_id'] = cluster_id
            item['cluster_name'] = cluster_name
        app_results[result_key].extend(item for item in data_list if isinstance(item, dict))

    def safe_call(endpoint_key: str, fn, *args, **kwargs):
        endpoint_attempted[endpoint_key] = True
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            logger.error("❌ Final failure for endpoint '%s' (attempt %s) after all retries: %s", endpoint_key, attempt_id, str(e), exc_info=False)
            _record_error(endpoint_key, fn, e, endpoint_errors)
            return None

    # --- Stage 1: Analyze Application Performance (using pre-fetched details) ---
    try:
        perf_analysis = analyzer.analyze_application_performance(app_details, attempt_id)
        add_and_append([perf_analysis], 'applications')
    except Exception as e:
        logger.error("❌ Failed during 'applications' post-processing for attempt %s: %s", attempt_id, str(e), exc_info=True)
        _record_error('applications', analyzer.analyze_application_performance, e, endpoint_errors)

    # --- Stage 2: Concurrently Fetch Primary Data Endpoints ---
    fetched_data = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6, thread_name_prefix="SHS_Primary") as executor:
        future_to_endpoint = {
            executor.submit(safe_call, 'jobs', shs_client.get_application_jobs, app_id, attempt_id): 'jobs',
            executor.submit(safe_call, 'stages', shs_client.get_application_stages, app_id, attempt_id): 'stages',
            executor.submit(safe_call, 'executors', shs_client.get_application_executors, app_id, attempt_id): 'executors',
            executor.submit(safe_call, 'sql', shs_client.get_application_sql_queries, app_id, attempt_id): 'sql'
        }
        for future in concurrent.futures.as_completed(future_to_endpoint):
            endpoint_key = future_to_endpoint[future]
            try:
                result = future.result()
                if result:
                    fetched_data[endpoint_key] = result
            except Exception as e:
                logger.error("❌ Exception retrieving result for endpoint '%s': %s", endpoint_key, str(e), exc_info=True)

    # --- Stage 3: Process Concurrently Fetched Data ---
    if 'jobs' in fetched_data:
        try:
            job_analysis = analyzer.analyze_job_performance(fetched_data['jobs'])
            add_and_append(job_analysis, 'jobs')
        except Exception as e:
            logger.error("❌ Failed during 'jobs' processing for attempt %s: %s", attempt_id, str(e), exc_info=True)
            _record_error('jobs', analyzer.analyze_job_performance, e, endpoint_errors)

    if 'executors' in fetched_data:
        try:
            executor_analysis = analyzer.analyze_executor_performance(fetched_data['executors'])
            add_and_append(executor_analysis, 'executors')
        except Exception as e:
            logger.error("❌ Failed during 'executors' processing for attempt %s: %s", attempt_id, str(e), exc_info=True)
            _record_error('executors', analyzer.analyze_executor_performance, e, endpoint_errors)

    if 'sql' in fetched_data:
        try:
            sql_analysis = analyzer.analyze_sql_queries(fetched_data['sql'])
            add_and_append(sql_analysis, 'sql_queries')
        except Exception as e:
            logger.error("❌ Failed during 'sql' processing for attempt %s: %s", attempt_id, str(e), exc_info=True)
            _record_error('sql', analyzer.analyze_sql_queries, e, endpoint_errors)

    # --- Stage 4: Process Stages and Concurrently Fetch Sub-tasks ---
    if 'stages' in fetched_data:
        stages_data = fetched_data['stages']
        try:
            stages_analysis = analyzer.analyze_stage_performance(stages_data)
            add_and_append(stages_analysis, 'stages')
        except Exception as e:
            logger.error("❌ Failed during 'stages' post-processing for attempt %s: %s", attempt_id, str(e), exc_info=True)
            _record_error('stages', analyzer.analyze_stage_performance, e, endpoint_errors)

        # Concurrently fetch tasks and summaries for all stages
        with concurrent.futures.ThreadPoolExecutor(max_workers=8, thread_name_prefix="SHS_SubTask") as executor:
            future_to_stage_task = {}
            for stage_raw in stages_data:
                stage_id = stage_raw.get('stageId')
                stage_attempt_id = stage_raw.get('attemptId', 0)
                if stage_id is None:
                    continue
                # Submit tasks and task_summaries calls for each stage
                future_to_stage_task[executor.submit(safe_call, 'tasks', shs_client.get_stage_tasks, app_id, attempt_id, stage_id, stage_attempt_id)] = ('tasks', stage_id, stage_attempt_id)
                future_to_stage_task[executor.submit(safe_call, 'task_summaries', shs_client.get_stage_task_summary, app_id, attempt_id, stage_id, stage_attempt_id)] = ('task_summaries', stage_id, stage_attempt_id)
            
            for future in concurrent.futures.as_completed(future_to_stage_task):
                endpoint_key, stage_id, stage_attempt_id = future_to_stage_task[future]
                try:
                    result = future.result()
                    if not result:
                        continue
                    
                    if endpoint_key == 'tasks':
                        for task in result:
                            task['stage_id'] = stage_id
                            task['stage_attempt_id'] = stage_attempt_id
                        task_analysis = analyzer.analyze_task_performance(result)
                        add_and_append(task_analysis, 'tasks')
                    elif endpoint_key == 'task_summaries':
                        result['stage_id'] = stage_id
                        result['stage_attempt_id'] = stage_attempt_id
                        summary_analysis = analyzer.analyze_task_summaries([result])
                        add_and_append(summary_analysis, 'task_summaries')

                except Exception as e:
                    logger.error("❌ Exception retrieving sub-task result for endpoint '%s' stage '%s': %s", endpoint_key, stage_id, str(e), exc_info=True)
    else:
        logger.warning("⚠️ No stage data returned from API for attempt %s. Skipping task and summary collection.", attempt_id)
    
    return app_results

def analyze_application_attempts(
    app_id: str,
    shs_client: Any,
    analyzer: Any,
    cluster_id: str,
    cluster_name: str,
    endpoint_errors: Dict[str, List[Dict[str, Any]]],
    endpoint_attempted: Dict[str, bool],
    endpoint_skipped_reasons: Dict[str, List[str]]
) -> List[Dict[str, Any]]:
    """
    Orchestrates the analysis of a single application by finding all successful attempts
    and processing each one individually.
    """
    all_attempt_results = []
    
    # This is a safecall wrapper for use inside this orchestrator function
    def orchestrator_safe_call(endpoint_key: str, fn, *args, **kwargs):
        endpoint_attempted[endpoint_key] = True
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            logger.error("API call for endpoint '%s' app '%s' failed: %s", endpoint_key, app_id, str(e), exc_info=True)
            _record_error(endpoint_key, fn, e, endpoint_errors)
            return None

    app_details = orchestrator_safe_call('applications', shs_client.get_application_details, app_id)

    if not app_details or not app_details.get('attempts'):
        logger.warning("Application %s has no details or attempts data. Skipping.", app_id)
        return []

    attempts = app_details.get('attempts', [])
    successful_attempts = [attempt for attempt in attempts if attempt.get('completed', False)]
    
    logger.info("Found %d total attempts for app %s. Will analyze %d successful attempts.", len(attempts), app_id, len(successful_attempts))
    if not successful_attempts:
        return []

    for attempt in successful_attempts:
        attempt_id = attempt.get('attemptId')
        if not attempt_id:
            continue
        
        attempt_result = process_single_application(
            app_id=app_id,
            attempt_id=attempt_id,
            shs_client=shs_client,
            analyzer=analyzer,
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            endpoint_errors=endpoint_errors,
            endpoint_attempted=endpoint_attempted,
            endpoint_skipped_reasons=endpoint_skipped_reasons,
            app_details=app_details
        )
        if attempt_result:
            all_attempt_results.append(attempt_result)
            
    return all_attempt_results

def analyze_single_cluster(
    cluster_info: Dict,
    timeout_seconds: int,
    max_applications: int,
    spark_session: SparkSession,
    persistent_ui_timeout: int
) -> Dict[str, Any]:
    """Analyzes a single EMR cluster. This version now uses an orchestrator to analyze all successful attempts for each application."""
    cluster_id = cluster_info['cluster_id']
    cluster_name = cluster_info['cluster_name']
    cluster_arn = cluster_info['cluster_arn']
    logger.info("Starting analysis for cluster: %s (%s)", cluster_name, cluster_id)

    results_aggregator = {
        'cluster_id': cluster_id, 'cluster_name': cluster_name, 'status': cluster_info.get('status', 'UNKNOWN'),
        'normalized_instance_hours': cluster_info.get('normalized_instance_hours', 0),
        'applications': [], 'jobs': [], 'stages': [], 'tasks': [], 'sql_queries': [], 'executors': [], 'task_summaries': [],
        'analysis_status': 'PENDING', 'error_message': ''
    }
    
    tracked_endpoints = ['applications', 'jobs', 'stages', 'tasks', 'sql', 'executors', 'task_summaries']
    endpoint_errors: Dict[str, List[Dict[str, Any]]] = {k: [] for k in tracked_endpoints}
    endpoint_attempted: Dict[str, bool] = {k: False for k in tracked_endpoints}
    endpoint_skipped_reasons: Dict[str, List[str]] = {k: [] for k in tracked_endpoints}
    
    # This is a safecall wrapper for use inside this orchestrator function
    def cluster_safe_call(endpoint_key: str, fn, *args, **kwargs):
        try:
            # Circuit breaker logic (if implemented in client)
            if hasattr(shs_client, 'should_skip') and shs_client.should_skip(endpoint_key):
                logger.warning("Skipping endpoint %s due to prior failures", endpoint_key)
                endpoint_skipped_reasons.setdefault(endpoint_key, []).append("Skipped due to prior failures (circuit breaker)")
                return None
        except UnboundLocalError:
            pass  # shs_client not yet defined
            
        endpoint_attempted[endpoint_key] = True
        try:
            result = fn(*args, **kwargs)
            try:
                if hasattr(shs_client, 'record_success'):
                    shs_client.record_success(endpoint_key)
            except Exception: pass
            return result
        except Exception as e:
            logger.error("Endpoint '%s' call failed: %s", endpoint_key, str(e), exc_info=True)
            _record_error(endpoint_key, fn, e, endpoint_errors)
            try:
                if hasattr(shs_client, 'record_failure'):
                    shs_client.record_failure(endpoint_key)
            except Exception: pass
            return None

    try:
        server_config = ServerConfig(emr_cluster_arn=cluster_arn, timeout=timeout_seconds)
        emr_client = EMRPersistentUIClient(server_config)
        base_url, session = emr_client.initialize(max_wait_time=persistent_ui_timeout)
        
        shs_client = SparkHistoryServerClient(base_url, session)
        
        analyzer = SparkMetricsAnalyzer(spark_session)
        
        applications = cluster_safe_call('applications', shs_client.get_applications, limit=max_applications)

        if not applications:
            logger.warning("No applications found in Spark History Server for %s", cluster_name)
            results_aggregator['analysis_status'] = 'NO_APPLICATIONS'
            results_aggregator['endpoint_errors'] = endpoint_errors
            results_aggregator['endpoint_attempted'] = endpoint_attempted
            results_aggregator['endpoint_skipped_reasons'] = endpoint_skipped_reasons
            return results_aggregator

        logger.info("Found %s applications to analyze in %s", len(applications), cluster_name)
        for app in applications:
            app_id = app.get('id')
            if not app_id:
                continue
            
            attempt_results_list = analyze_application_attempts(
                app_id, shs_client, analyzer, cluster_id, cluster_name,
                endpoint_errors, endpoint_attempted, endpoint_skipped_reasons
            )
            
            for app_data in attempt_results_list:
                if app_data:
                    for key in ['applications', 'jobs', 'stages', 'tasks', 'sql_queries', 'executors', 'task_summaries']:
                        results_aggregator[key].extend(app_data.get(key, []))

        # Finalize status
        if not any(results_aggregator.get(key) for key in ['applications', 'jobs', 'stages', 'tasks', 'sql_queries', 'executors', 'task_summaries']):
            results_aggregator['analysis_status'] = 'FAILED'
            if not results_aggregator['error_message']:
                results_aggregator['error_message'] = "No data returned from SHS endpoints for any successful attempts"
            logger.warning("Cluster %s analysis produced no data across all endpoints.", cluster_name)
        else:
            results_aggregator['analysis_status'] = 'COMPLETED'
        
        logger.info("Completed analysis for cluster: %s", cluster_name)
    except Exception as e:
        logger.error("Failed to analyze cluster %s: %s", cluster_name, str(e), exc_info=True)
        results_aggregator['analysis_status'] = 'FAILED'
        results_aggregator['error_message'] = str(e)

    results_aggregator['endpoint_errors'] = endpoint_errors
    results_aggregator['endpoint_attempted'] = endpoint_attempted
    results_aggregator['endpoint_skipped_reasons'] = endpoint_skipped_reasons
    return results_aggregator

def process_clusters_in_batches(
    clusters_to_analyze: List[Dict],
    batch_size: int,
    batch_delay_seconds: int,
    spark_session: Any
) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict], List[Dict]]:
    """Process clusters in sequential batches to manage resources and API limits. This version builds a JSON-as-string status_details per cluster with endpoint-level OK/FAILED/SKIPPED."""
    all_results = {'applications': [], 'jobs': [], 'stages': [], 'tasks': [], 'sql_queries': [], 'executors': [], 'task_summaries': []}
    cluster_summaries = []

    def endpoint_status_summary(c: Dict[str, Any]) -> str:
        key_map = {'applications': 'applications', 'jobs': 'jobs', 'stages': 'stages', 'tasks': 'tasks', 'sql': 'sql_queries', 'executors': 'executors', 'task_summaries': 'task_summaries'}
        attempted = c.get('endpoint_attempted', {})
        errors = c.get('endpoint_errors', {})
        skipped = c.get('endpoint_skipped_reasons', {})
        status_obj = {}
        for ep, data_key in key_map.items():
            has_data = len(c.get(data_key, [])) > 0
            ep_attempted = bool(attempted.get(ep, False))
            ep_errors = errors.get(ep, [])
            ep_skips = skipped.get(ep, [])
            if has_data:
                status_obj[ep] = 'OK'
            elif ep_attempted and len(ep_errors) > 0:
                # Extract just the error codes from the error objects
                error_codes = [err.get('code', 'UNKNOWN') for err in ep_errors[:MAX_ENDPOINT_FAILURES]]
                status_obj[ep] = f"FAILED({','.join(error_codes)})"
            else:
                # Use a simplified reason
                reason = ep_skips[0] if ep_skips else "No data"
                # Truncate long reasons to keep it concise
                if len(reason) > 30:
                    reason = reason[:27] + "..."
                status_obj[ep] = f"SKIPPED({reason})"
        return json.dumps(status_obj, separators=(',', ':'))

    total_batches = (len(clusters_to_analyze) + batch_size - 1) // batch_size
    for i in range(0, len(clusters_to_analyze), batch_size):
        batch_clusters = clusters_to_analyze[i:i + batch_size]
        current_batch_num = (i // batch_size) + 1
        logger.info("Processing batch %d/%d...", current_batch_num, total_batches)

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(batch_clusters)) as executor:
            future_to_cluster = {executor.submit(analyze_single_cluster, c_info, TIMEOUT_SECONDS, MAX_APPLICATIONS, spark_session, PERSISTENT_UI_TIMEOUT_SECONDS): c_info for c_info in batch_clusters}
            
            for future in concurrent.futures.as_completed(future_to_cluster):
                cluster_info = future_to_cluster[future]
                try:
                    cluster_results = future.result()
                    if cluster_results:
                        for key in all_results.keys():
                            all_results[key].extend(cluster_results.get(key, []))
                        
                        cluster_summaries.append({
                            'cluster_id': cluster_results['cluster_id'],
                            'cluster_name': cluster_results['cluster_name'],
                            'status': cluster_results['status'],
                            'analysis_status': cluster_results['analysis_status'],
                            'status_details': endpoint_status_summary(cluster_results),
                            'normalized_instance_hours': cluster_results.get('normalized_instance_hours', 0),
                            'total_applications': len(cluster_results['applications']),
                            'total_jobs': len(cluster_results['jobs']),
                            'total_stages': len(cluster_results['stages']),
                            'total_tasks': len(cluster_results['tasks']),
                            'total_sql_queries': len(cluster_results['sql_queries']),
                            'total_executors': len(cluster_results['executors']),
                            'total_task_summaries': len(cluster_results['task_summaries']),
                        })
                except Exception as e:
                    logger.error("Error processing results for cluster %s: %s", cluster_info['cluster_id'], str(e), exc_info=True)
                    cluster_summaries.append({
                        'cluster_id': cluster_info['cluster_id'],
                        'cluster_name': cluster_info['cluster_name'],
                        'status': 'FAILED_PROCESSING',
                        'analysis_status': 'FAILED',
                        'status_details': json.dumps({'error': _truncate_message(str(e))}),
                        'normalized_instance_hours': cluster_info.get('normalized_instance_hours', 0),
                        'total_applications': 0, 'total_jobs': 0, 'total_stages': 0,
                        'total_tasks': 0, 'total_sql_queries': 0, 'total_executors': 0, 'total_task_summaries': 0
                    })

        if current_batch_num < total_batches:
            logger.info("Waiting %d seconds between batches...", batch_delay_seconds)
            time.sleep(batch_delay_seconds)
            
    return all_results['applications'], all_results['jobs'], all_results['stages'], all_results['tasks'], all_results['sql_queries'], all_results['executors'], all_results['task_summaries'], cluster_summaries


# COMMAND ----------

# MAGIC %md
# MAGIC ### Main Execution

# COMMAND ----------

# DBTITLE 1,Main Execution
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

def main_analysis():
    """Main function to drive the EMR cluster analysis."""
    try:
        # Step 1: Discover Clusters
        emr_discovery = EMRClusterDiscovery(region=AWS_REGION)
        total_clusters_discovered = 0
        
        if EMR_CLUSTER_ARN:
            logger.info("Analyzing single specified EMR cluster: %s", EMR_CLUSTER_ARN)
            cluster_details = emr_discovery.get_cluster_details(EMR_CLUSTER_ARN.split('/')[-1])
            clusters_to_analyze = [cluster_details] if cluster_details else []
            total_clusters_discovered = 1 if cluster_details else 0
        else:
            logger.info("Discovering EMR clusters based on specified criteria...")
            discovered_clusters = emr_discovery.discover_clusters(
                states=CLUSTER_STATES_LIST,
                name_filter=CLUSTER_NAME_FILTER,
                max_clusters=MAX_CLUSTERS,
                created_after=PARSED_CREATED_AFTER_DATE,
                created_before=PARSED_CREATED_BEFORE_DATE,
                custom_hours_threshold=CUSTOM_HOURS_THRESHOLD
            )
            total_clusters_discovered = len(discovered_clusters)
            
            # Step 2: Validate and Get Details for Discovered Clusters
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_cluster = {executor.submit(emr_discovery.get_cluster_details, c['cluster_id']): c for c in discovered_clusters}
                detailed_clusters = [future.result() for future in concurrent.futures.as_completed(future_to_cluster)]
            
            clusters_to_analyze = [c for c in detailed_clusters if emr_discovery.validate_cluster_for_analysis(c)]

        if not clusters_to_analyze:
            logger.warning("No valid clusters found for analysis.")
            return {
                'summary': {
                    'total_clusters_discovered': total_clusters_discovered,
                    'total_clusters_analyzed': 0,
                    'clusters_fully_analyzed': 0,
                    'cluster_analysis_success_rate': 0.0
                }
            }
        
        logger.info("Will analyze %s clusters.", len(clusters_to_analyze))

        # Step 3: Process clusters in batches
        all_apps, all_jobs, all_stages, all_tasks, all_sql, all_execs, all_task_sums, summaries = process_clusters_in_batches(
            clusters_to_analyze, BATCH_SIZE, BATCH_DELAY_SECONDS, spark
        )

        # Step 4: Create DataFrames
        logger.info("Creating analysis DataFrames...")
        analyzer = SparkMetricsAnalyzer(spark)
        apps_df, jobs_df, stages_df, tasks_df, sql_df, exec_df, task_sum_df = analyzer.create_dynamic_dataframes(
            all_apps, all_jobs, all_stages, all_tasks, all_sql, all_execs, all_task_sums
        )

        cluster_summary_df = None
        if summaries:
            summary_schema = StructType([
                StructField("cluster_id", StringType(), True), StructField("cluster_name", StringType(), True),
                StructField("status", StringType(), True), StructField("analysis_status", StringType(), True),
                StructField("status_details", StringType(), True),
                StructField("normalized_instance_hours", IntegerType(), True),
                StructField("total_applications", IntegerType(), True), StructField("total_jobs", IntegerType(), True),
                StructField("total_stages", IntegerType(), True), StructField("total_tasks", IntegerType(), True),
                StructField("total_sql_queries", IntegerType(), True), StructField("total_executors", IntegerType(), True),
                StructField("total_task_summaries", IntegerType(), True)
            ])
            cluster_summary_df = spark.createDataFrame(summaries, schema=summary_schema)

        # Step 5: Final Summary
        total_clusters_analyzed = len([c for c in (summaries or []) if c.get('analysis_status') == 'COMPLETED'])
        
        # Calculate clusters_fully_analyzed - clusters with data from all major endpoints
        clusters_fully_analyzed = 0
        if summaries:
            for cluster_summary in summaries:
                # A cluster is considered fully analyzed if it has data from all major endpoints
                has_all_data = (
                    cluster_summary.get('total_applications', 0) > 0 and
                    cluster_summary.get('total_jobs', 0) > 0 and
                    cluster_summary.get('total_stages', 0) > 0 and
                    cluster_summary.get('total_tasks', 0) > 0 and
                    cluster_summary.get('total_sql_queries', 0) > 0 and
                    cluster_summary.get('total_executors', 0) > 0 and
                    cluster_summary.get('total_task_summaries', 0) > 0
                )
                if has_all_data:
                    clusters_fully_analyzed += 1
        
        # Calculate success rate
        cluster_analysis_success_rate = 0.0
        if total_clusters_analyzed > 0:
            cluster_analysis_success_rate = round((clusters_fully_analyzed / total_clusters_analyzed) * 100.0, 2)
        
        final_summary = {
            'clusters_discovered_count': total_clusters_discovered,
            'clusters_extracted_count': total_clusters_analyzed,
            'clusters_fully_analyzed_count': clusters_fully_analyzed,
            'cluster_analysis_success_rate %': cluster_analysis_success_rate,
            'total_applications': len(all_apps),
            'total_jobs': len(all_jobs),
            'total_stages': len(all_stages),
            'total_tasks': len(all_tasks),
            'total_sql_queries': len(all_sql),
            'total_executors': len(all_execs),
            'total_task_summaries': len(all_task_sums)
        }

        return {
            "cluster_summaries_df": cluster_summary_df,
            "applications_df": apps_df,
            "jobs_df": jobs_df,
            "stages_df": stages_df,
            "tasks_df": tasks_df,
            "sql_df": sql_df,
            "executors_df": exec_df,
            "task_summaries_df": task_sum_df,
            "summary": final_summary
        }
    except Exception as e:
        logger.error("Main analysis failed: %s", str(e), exc_info=True)
        raise

# Execute main analysis
results = main_analysis()

# Display summary and make DataFrames available
if results:
    cluster_summaries_df = results.get('cluster_summaries_df')
    applications_df = results.get('applications_df')
    jobs_df = results.get('jobs_df')
    stages_df = results.get('stages_df')
    tasks_df = results.get('tasks_df')
    sql_df = results.get('sql_df')
    executors_df = results.get('executors_df')
    task_summaries_df = results.get('task_summaries_df')
    analysis_summary = results.get('summary', {})
    
    print("-" * 100)
    print("EMR SPARK HISTORY ANALYSIS COMPLETED!")
    print("-" * 100)
    for key, value in analysis_summary.items():
        print(f"  {key.replace('_', ' ').title()}: {value}")
    print("\nDataFrames available for analysis: cluster_summaries_df, applications_df, jobs_df, stages_df, tasks_df, sql_df, executors_df, task_summaries_df")

    # Write outputs with date-based versioning in prod
    if ENVIRONMENT == "prod" and OUTPUT_RUN_PATH:
        logger.info("Writing analysis results to S3: %s", OUTPUT_RUN_PATH)
        try:
            for df_name, df_instance in results.items():
                if df_name.endswith('_df') and df_instance:
                    df_instance.write.mode('overwrite').parquet(f"{OUTPUT_RUN_PATH}/{df_name.replace('_df', '')}")
            logger.info("All analysis results successfully written to S3.")
        except Exception as e:
            print(f"\n❌ ANALYSIS FAILED: {str(e)} - check the logs above for detailed error information.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Exploration

# COMMAND ----------

# DBTITLE 1,Analysis Summary
# The analysis_summary dictionary contains the final counts.
if 'analysis_summary' in locals() and analysis_summary:
    display(spark.createDataFrame([analysis_summary]))
else:
    print("Analysis summary is not available.")

# COMMAND ----------

# DBTITLE 1,Cluster Summaries
if 'cluster_summaries_df' in locals() and cluster_summaries_df:
    display(cluster_summaries_df)
else:
    print("Cluster summaries DataFrame is not available.")

# COMMAND ----------

# DBTITLE 1,Applications
if 'applications_df' in locals() and applications_df:
    display(applications_df)
else:
    print("Applications DataFrame is not available.")

# COMMAND ----------

# DBTITLE 1,Executors
if 'executors_df' in locals() and executors_df:
    display(executors_df)
else:
    print("Executors DataFrame is not available.")

# COMMAND ----------

# DBTITLE 1,Jobs
if 'jobs_df' in locals() and jobs_df:
    display(jobs_df)
else:
    print("Jobs DataFrame is not available.")

# COMMAND ----------

# DBTITLE 1,Stages
if 'stages_df' in locals() and stages_df:
    display(stages_df)
else:
    print("Stages DataFrame is not available.")

# COMMAND ----------

# DBTITLE 1,Tasks
if 'tasks_df' in locals() and tasks_df:
    display(tasks_df)
else:
    print("Tasks DataFrame is not available.")

# COMMAND ----------

# DBTITLE 1,Task Summaries
if 'task_summaries_df' in locals() and task_summaries_df:
    display(task_summaries_df)
else:
    print("Task summaries DataFrame is not available.")

# COMMAND ----------

# DBTITLE 1,SQL Queries
if 'sql_df' in locals() and sql_df:
    display(sql_df)
else:
    print("SQL queries DataFrame is not available.")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, StringType
# Step 1: Parse JSON and explode the nodes array (equivalent to lateral view explode)
raw_df = sql_df.withColumn("parsed_json", F.from_json(F.col("sql_raw_json"), 
                       StructType([
                           StructField("nodes", ArrayType(
                               StructType([
                                   StructField("nodeId", IntegerType(), True),
                                   StructField("nodeName", StringType(), True),
                                   StructField("metrics", ArrayType(
                                       StructType([
                                           StructField("name", StringType(), True),
                                           StructField("value", StringType(), True)
                                       ])
                                   ), True)
                               ])
                           ), True)
                       ]))) \
         .select("*", F.explode(F.col("parsed_json.nodes")).alias("nodemetrics"))

# Step 2: Create photon check with case when logic
photoncheck_df = raw_df.withColumn("photonbinary", 
    F.when(F.col("nodemetrics.nodeName") == "MapElements", 0)
     .when(F.col("nodemetrics.nodeName") == "MapPartitions", 0)
     .when(F.col("nodemetrics.nodeName") == "Scan csv", 0)
     .when(F.col("nodemetrics.nodeName") == "Scan json", 0)
     .when(F.col("nodemetrics.nodeName") == "PythonUDF", 0)
     .when(F.col("nodemetrics.nodeName") == "ScalaUDF", 0)
     .when(F.col("nodemetrics.nodeName") == "FlatMapGroupsInPandas", 0)
     .when(F.col("nodemetrics.nodeName") == "DeserializeToObject", 0)
     .when(F.col("nodemetrics.nodeName") == "SerializeFromObject", 0)
     .otherwise(1)
)

# Step 3: Calculate job photon percentage (equivalent to jobcheck CTE)
jobcheck_df = photoncheck_df.groupBy("cluster_name", "application_id") \
                             .agg((F.sum("photonbinary") / F.count("*")).alias("jobphotonperc"))

# Step 4: Final result with any_value equivalent
result_df = jobcheck_df.groupBy("cluster_name", "application_id") \
                       .agg(F.first("jobphotonperc").alias("jobphotonperc"))

# Display the result
display(result_df)

# COMMAND ----------

joined_df = cluster_summaries_df.join(applications_df, cluster_summaries_df.cluster_id == applications_df.cluster_id, how='right')
final_df = joined_df.join(result_df, ['cluster_name'], how='inner')
display(final_df)