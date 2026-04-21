from __future__ import annotations

import csv
import io
import os
from datetime import datetime, timedelta
from typing import Optional

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

# Import shared utilities
from datahub_metadata_utils import (
    get_minio_client,
    find_latest_metadata_file,
    process_excel_to_ansi_csv_bytes,
    update_dataset_in_datahub
)

def _var(name: str, default: Optional[str] = None) -> Optional[str]:
    try:
        return Variable.get(name)
    except Exception:
        return os.getenv(name, default)

def _load_settings():
    # Credentials now come from 'minio_conn' Connection
    return {
        "minio_conn_id": "minio_conn",
        "minio_bucket": _var("MINIO_BUCKET", "infoschema"),
        "datahub_gms_endpoint": _var("DATAHUB_GMS_ENDPOINT", "http://host.docker.internal:8080"),
        "datahub_env": _var("DATAHUB_ENV", "PROD"),
        "archive_processed": (_var("ARCHIVE_PROCESSED", "true") or "").lower() in ("1", "true", "yes", "y"),
    }

with DAG(
    dag_id="datahub_minio_dynamic_metadata_sync",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Trigger manually or set a schedule
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["datahub", "minio", "metadata", "excel", "csv"],
) as dag:

    @task
    def process_metadata_sync() -> None:
        settings = _load_settings()
        s3 = get_minio_client(settings["minio_conn_id"])
        bucket = settings["minio_bucket"]
        
        # 1. Find the latest metadata file based on YYMMDD pattern and priority (xlsx > csv)
        target_key, ext = find_latest_metadata_file(s3, bucket)
        
        if not target_key:
            raise AirflowSkipException("No metadata files matching 'update_metadata_YYMMDD.(csv|xlsx)' found.")
        
        print(f"Latest file found: {target_key} (Extension: {ext})")

        # 2. If Excel, convert to ANSI CSV and save back to MinIO
        if ext == "xlsx":
            print(f"Processing Excel file: {target_key}")
            obj = s3.get_object(Bucket=bucket, Key=target_key)
            excel_bytes = obj["Body"].read()
            
            # Convert to ANSI (CP949) CSV bytes
            csv_bytes = process_excel_to_ansi_csv_bytes(excel_bytes)
            
            # Define output CSV key (same name, different extension)
            csv_key = target_key.replace(".xlsx", ".csv")
            
            print(f"Uploading converted ANSI CSV to: {csv_key}")
            s3.put_object(Bucket=bucket, Key=csv_key, Body=csv_bytes)
            
            # Now proceed with the CSV bytes for DataHub update
            processing_bytes = csv_bytes
            source_key = csv_key # We'll archive/use this one as the primary record
        else:
            # It's a CSV already
            print(f"Processing CSV file: {target_key}")
            obj = s3.get_object(Bucket=bucket, Key=target_key)
            processing_bytes = obj["Body"].read()
            source_key = target_key

        # 3. Parse CSV rows (handling ANSI/CP949 or UTF-8-SIG)
        try:
            # Try CP949 first (since we just saved it or user might have uploaded ANSI)
            text = processing_bytes.decode("cp949")
        except UnicodeDecodeError:
            # Fallback to UTF-8-SIG (common for Excel CSVs with BOM)
            text = processing_bytes.decode("utf-8-sig")

        reader = csv.DictReader(io.StringIO(text))
        rows = [{(k or "").strip(): (v or "") for k, v in r.items() if k} for r in reader]
        
        if not rows:
            raise AirflowSkipException(f"File {source_key} has no valid rows.")

        # 4. Group by URN and update DataHub
        from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
        
        dh_config = DatahubClientConfig(server=settings["datahub_gms_endpoint"])
        graph = DataHubGraph(dh_config)
        
        by_urn: dict[str, list] = {}
        for r in rows:
            urn = (r.get("urn") or "").strip()
            if urn:
                by_urn.setdefault(urn, []).append(r)
        
        for urn, items in by_urn.items():
            update_dataset_in_datahub(graph, urn=urn, row_items=items, env=settings["datahub_env"])
            
        print(f"Successfully updated {len(by_urn)} datasets in DataHub.")

        # 5. Archive the processed files
        if settings["archive_processed"]:
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            # Archive original file (xlsx or csv)
            archive_key_orig = f"archive/{ts}-{os.path.basename(target_key)}"
            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": target_key},
                Key=archive_key_orig
            )
            s3.delete_object(Bucket=bucket, Key=target_key)
            
            # If we created a CSV from XLSX, archive/delete that too
            if ext == "xlsx":
                # The csv file we created
                csv_key = target_key.replace(".xlsx", ".csv")
                archive_key_csv = f"archive/{ts}-generated-{os.path.basename(csv_key)}"
                s3.copy_object(
                    Bucket=bucket,
                    CopySource={"Bucket": bucket, "Key": csv_key},
                    Key=archive_key_csv
                )
                s3.delete_object(Bucket=bucket, Key=csv_key)

    process_metadata_sync()
