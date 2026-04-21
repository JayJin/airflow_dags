from __future__ import annotations

import csv
import io
import os
import re
from datetime import datetime, timedelta
from typing import Optional, Iterable, List, Dict, Any, Tuple

import pandas as pd
import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.hooks.base import BaseHook

# --- [내부 유틸리티 함수] ---

# Robust parsing for tags/terms
_TAG_TOKEN_RE = re.compile(r"\"([^\"]+)\"|'([^']+)'|([^,\s\( \)\"']+)")

def _parse_multi_value(value: Optional[Any]) -> list[str]:
    if value is None: return []
    raw = str(value).strip()
    if not raw or raw.lower() in ('nan', 'none'): return []
    
    if (raw.startswith("(") and raw.endswith(")")) or (raw.startswith("[") and raw.endswith("]")):
        inner = raw[1:-1].strip()
    else:
        inner = raw
        
    tokens: list[str] = []
    for m in _TAG_TOKEN_RE.finditer(inner):
        token = (m.group(1) or m.group(2) or m.group(3) or "").strip()
        if token: tokens.append(token)
            
    seen: set[str] = set()
    out: list[str] = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

def _normalize_id(value: str) -> str:
    v = str(value).strip()
    v = re.sub(r"\s+", "_", v)
    v = re.sub(r"[^A-Za-z0-9_.-]", "_", v)
    v = re.sub(r"_+", "_", v).strip("_")
    return v

def _corpuser_urn(owner: str) -> str:
    o = owner.strip()
    return o if o.startswith("urn:li:corpuser:") else f"urn:li:corpuser:{_normalize_id(o)}"

def _domain_urn(domain: str) -> str:
    d = domain.strip()
    return d if d.startswith("urn:li:domain:") else f"urn:li:domain:{_normalize_id(d)}"

def _tag_urn(tag: str) -> str:
    t = tag.strip()
    return t if t.startswith("urn:li:tag:") else f"urn:li:tag:{_normalize_id(t)}"

def _term_urn(term: str) -> str:
    t = term.strip()
    return t if t.startswith("urn:li:glossaryTerm:") else f"urn:li:glossaryTerm:{_normalize_id(t)}"

def _merge_unique(existing: list[str], new: list[str]) -> list[str]:
    seen = set(existing)
    out = list(existing)
    for x in new:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def get_minio_client(conn_id: str):
    """Airflow Connection에서 정보를 가져와 Boto3 S3 클라이언트를 생성합니다."""
    conn = BaseHook.get_connection(conn_id)
    endpoint = conn.host
    if endpoint and not endpoint.startswith("http"):
        endpoint = f"http://{endpoint}"
    if conn.port and endpoint and f":{conn.port}" not in endpoint:
        endpoint = f"{endpoint}:{conn.port}"
        
    extra = conn.extra_dejson
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name=extra.get("region_name", "us-east-1")
    )

def ensure_tag_entities(graph, tags: Iterable[str]) -> None:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import TagPropertiesClass
    for tag in tags:
        tag_id = _normalize_id(tag)
        if not tag_id: continue
        graph.emit(MetadataChangeProposalWrapper(
            entityUrn=_tag_urn(tag), aspect=TagPropertiesClass(name=tag)
        ))

def update_dataset_in_datahub(graph, urn: str, row_items: list[dict[str, Any]], env: str) -> None:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        DomainsClass, EditableDatasetPropertiesClass, EditableSchemaFieldInfoClass,
        EditableSchemaMetadataClass, GlobalTagsClass, GlossaryTermAssociationClass,
        GlossaryTermsClass, OwnerClass, OwnershipClass, OwnershipTypeClass, TagAssociationClass,
    )

    table_desc, table_owner, table_domain = "", "", ""
    table_tags, table_terms = [], []
    field_updates: dict[str, dict[str, Any]] = {}
    
    for r in row_items:
        td = str(r.get("table_desc") or "").strip()
        if td: table_desc = td
        to = str(r.get("table_owner") or "").strip()
        if to: table_owner = to
        dom = str(r.get("table_domain") or r.get("talbe_domain") or "").strip()
        if dom: table_domain = dom
        table_tags = _merge_unique(table_tags, _parse_multi_value(r.get("table_tags")))
        table_terms = _merge_unique(table_terms, _parse_multi_value(r.get("table_terms")))

        col = str(r.get("col_name") or r.get("column_name") or "").strip()
        if not col: continue
        upd = field_updates.setdefault(col, {"desc": "", "tags": [], "terms": []})
        cd = str(r.get("col_desc") or r.get("column_desc") or "").strip()
        if cd: upd["desc"] = cd
        upd["tags"] = _merge_unique(upd["tags"], _parse_multi_value(r.get("col_tags") or r.get("column_tags")))
        upd["terms"] = _merge_unique(upd["terms"], _parse_multi_value(r.get("col_terms") or r.get("column_terms")))

    # Update Aspects
    if table_desc:
        graph.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=EditableDatasetPropertiesClass(description=table_desc)))
    if table_owner:
        owner_urn = _corpuser_urn(table_owner)
        existing = graph.get_aspect(entity_urn=urn, aspect_type=OwnershipClass)
        owners = list(existing.owners) if existing and getattr(existing, "owners", None) else []
        if not any(getattr(o, "owner", "") == owner_urn for o in owners):
            owners.append(OwnerClass(owner=owner_urn, type=OwnershipTypeClass.DATAOWNER))
            graph.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=OwnershipClass(owners=owners)))
    if table_domain:
        graph.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=DomainsClass(domains=[_domain_urn(table_domain)])))
    if table_tags:
        ensure_tag_entities(graph, table_tags)
        new_tag_urns = [_tag_urn(t) for t in table_tags if _normalize_id(t)]
        existing = graph.get_aspect(entity_urn=urn, aspect_type=GlobalTagsClass)
        existing_urns = [ta.tag for ta in existing.tags if getattr(ta, "tag", None)] if existing and getattr(existing, "tags", None) else []
        merged = _merge_unique(existing_urns, new_tag_urns)
        graph.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=GlobalTagsClass(tags=[TagAssociationClass(tag=u) for u in merged])))
    if table_terms:
        new_term_urns = [_term_urn(t) for t in table_terms if _normalize_id(t)]
        existing = graph.get_aspect(entity_urn=urn, aspect_type=GlossaryTermsClass)
        existing_urns = [ta.urn for ta in existing.terms if getattr(ta, "urn", None)] if existing and getattr(existing, "terms", None) else []
        merged = _merge_unique(existing_urns, new_term_urns)
        graph.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=GlossaryTermsClass(
            terms=[GlossaryTermAssociationClass(urn=u) for u in merged],
            auditStamp={"time": 0, "actor": "urn:li:corpuser:airflow"}
        )))

    # Column Level
    if field_updates:
        existing = graph.get_aspect(entity_urn=urn, aspect_type=EditableSchemaMetadataClass)
        existing_map: Dict[str, EditableSchemaFieldInfoClass] = {}
        if existing and getattr(existing, "editableSchemaFieldInfo", None):
            for f in existing.editableSchemaFieldInfo:
                if getattr(f, "fieldPath", None): existing_map[f.fieldPath] = f
        
        all_col_tags = []
        for upd in field_updates.values(): all_col_tags = _merge_unique(all_col_tags, upd["tags"])
        if all_col_tags: ensure_tag_entities(graph, all_col_tags)

        for f_path, upd in field_updates.items():
            desc = str(upd.get("desc") or "").strip()
            tag_urns = [_tag_urn(t) for t in upd["tags"] if _normalize_id(t)]
            term_urns = [_term_urn(t) for t in upd["terms"] if _normalize_id(t)]
            prev = existing_map.get(f_path)
            # Tags
            p_tags = [ta.tag for ta in prev.globalTags.tags if getattr(ta, "tag", None)] if prev and getattr(prev, "globalTags", None) and getattr(prev.globalTags, "tags", None) else []
            m_tags = _merge_unique(p_tags, tag_urns)
            g_tags = GlobalTagsClass(tags=[TagAssociationClass(tag=u) for u in m_tags]) if m_tags else None
            # Terms
            p_terms = [ta.urn for ta in prev.glossaryTerms.terms if getattr(ta, "urn", None)] if prev and getattr(prev, "glossaryTerms", None) and getattr(prev.glossaryTerms, "terms", None) else []
            m_terms = _merge_unique(p_terms, term_urns)
            g_terms = GlossaryTermsClass(terms=[GlossaryTermAssociationClass(urn=u) for u in m_terms], auditStamp={"time": 0, "actor": "urn:li:corpuser:airflow"}) if m_terms else None

            existing_map[f_path] = EditableSchemaFieldInfoClass(
                fieldPath=f_path, description=desc or (getattr(prev, "description", "") if prev else ""),
                globalTags=g_tags, glossaryTerms=g_terms
            )
        graph.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=EditableSchemaMetadataClass(editableSchemaFieldInfo=list(existing_map.values()))))

# --- [DAG 정의] ---

def _var(name: str, default: Optional[str] = None) -> Optional[str]:
    try: return Variable.get(name)
    except: return os.getenv(name, default)

with DAG(
    dag_id="datahub_minio_metadata_sync_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["datahub", "minio", "metadata"],
) as dag:

    @task
    def check_latest_file() -> dict:
        """1. Minio 연결 및 최신 소스 파일 탐색"""
        conn_id = "minio_conn"
        bucket = _var("MINIO_BUCKET", "datahub-infoschema")
        
        s3 = get_minio_client(conn_id)
        response = s3.list_objects_v2(Bucket=bucket, Prefix="update_metadata_")
        
        if 'Contents' not in response:
            raise AirflowSkipException("No metadata files found matching the pattern.")
        
        pattern = re.compile(r"update_metadata_(\d{6})\.(csv|xlsx)$")
        candidates = []
        for obj in response['Contents']:
            key = obj['Key']
            match = pattern.search(key)
            if match:
                candidates.append({"key": key, "date": match.group(1), "ext": match.group(2)})
        
        if not candidates:
            raise AirflowSkipException("No valid candidate files found.")
            
        candidates.sort(key=lambda x: (x['date'], x['ext']), reverse=True)
        best = candidates[0]
        
        print(f"Target file identified: {best['key']}")
        return {"key": best['key'], "ext": best['ext']}

    @task
    def transform_to_ansi_csv(file_info: dict) -> dict:
        """2. Excel일 경우 ANSI CSV로 변환 및 Minio 저장 (CSV일 경우 Pass)"""
        conn_id = "minio_conn"
        bucket = _var("MINIO_BUCKET", "datahub-infoschema")
        s3 = get_minio_client(conn_id)
        
        target_key = file_info["key"]
        ext = file_info["ext"]
        
        if ext == "xlsx":
            print(f"Converting Excel to ANSI CSV: {target_key}")
            excel_bytes = s3.get_object(Bucket=bucket, Key=target_key)["Body"].read()
            df = pd.read_excel(io.BytesIO(excel_bytes))
            output = io.StringIO()
            df.to_csv(output, index=False, encoding='cp949')
            csv_bytes = output.getvalue().encode('cp949')
            
            csv_key = target_key.replace(".xlsx", ".csv")
            s3.put_object(Bucket=bucket, Key=csv_key, Body=csv_bytes)
            print(f"Saved converted CSV to Minio: {csv_key}")
            return {"csv_key": csv_key, "original_key": target_key, "ext": ext}
        else:
            print(f"File is already CSV: {target_key}. Skipping transformation.")
            return {"csv_key": target_key, "original_key": target_key, "ext": ext}

    @task
    def sync_to_datahub(process_info: dict) -> None:
        """3. DataHub에 메타데이터 동기화"""
        conn_id = "minio_conn"
        bucket = _var("MINIO_BUCKET", "datahub-infoschema")
        gms_endpoint = _var("DATAHUB_GMS_ENDPOINT", "http://datahub-gms:8080")
        env = _var("DATAHUB_ENV", "PROD")
        
        s3 = get_minio_client(conn_id)
        csv_key = process_info["csv_key"]
        
        print(f"Fetching CSV data from: {csv_key}")
        processing_bytes = s3.get_object(Bucket=bucket, Key=csv_key)["Body"].read()
        
        try: text = processing_bytes.decode("cp949")
        except: text = processing_bytes.decode("utf-8-sig")

        reader = csv.DictReader(io.StringIO(text))
        rows = [{(k or "").strip(): (v or "") for k, v in r.items() if k} for r in reader]
        
        if not rows:
            raise AirflowSkipException(f"CSV {csv_key} has no valid data.")

        from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
        graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint))
        
        by_urn: dict[str, list] = {}
        for r in rows:
            urn = (r.get("urn") or "").strip()
            if urn: by_urn.setdefault(urn, []).append(r)
        
        for urn, items in by_urn.items():
            update_dataset_in_datahub(graph, urn=urn, row_items=items, env=env)
            
        print(f"Successfully processed {len(by_urn)} datasets to DataHub.")

    @task
    def archive_and_cleanup(process_info: dict) -> None:
        """4. 처리 완료된 파일 아카이브 및 정리"""
        conn_id = "minio_conn"
        bucket = _var("MINIO_BUCKET", "datahub-infoschema")
        archive_processed = (_var("ARCHIVE_PROCESSED", "true") or "").lower() in ("1", "true", "yes", "y")
        
        if not archive_processed:
            print("Archive setting disabled. Skipping cleanup.")
            return
            
        s3 = get_minio_client(conn_id)
        original_key = process_info["original_key"]
        csv_key = process_info["csv_key"]
        ext = process_info["ext"]
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        
        # Original 파일 보관
        s3.copy_object(
            Bucket=bucket, 
            CopySource={"Bucket": bucket, "Key": original_key}, 
            Key=f"archive/{ts}-{os.path.basename(original_key)}"
        )
        s3.delete_object(Bucket=bucket, Key=original_key)
        
        # Excel에서 생성된 CSV가 있다면 추가 보관/정리
        if ext == "xlsx":
            s3.copy_object(
                Bucket=bucket, 
                CopySource={"Bucket": bucket, "Key": csv_key}, 
                Key=f"archive/{ts}-gen-{os.path.basename(csv_key)}"
            )
            s3.delete_object(Bucket=bucket, Key=csv_key)
            
        print(f"Cleanup complete for {original_key}.")
    
    # 1. 파일 탐색
    file_info = check_latest_file()
    
    # 2. 형식 변환
    process_info = transform_to_ansi_csv(file_info)
    
    # 3. 데이터 업데이트
    sync_done = sync_to_datahub(process_info)
    
    # 4. 파일 정리 (3단계 업데이트가 성공한 후 파일 아카이브 및 삭제 진행)
    cleanup_done = archive_and_cleanup(process_info)
    
    # 명시적 의존성 연결: 화살표가 순서대로 이어지도록 보장
    sync_done >> cleanup_done
