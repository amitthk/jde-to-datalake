"""
S3 Data Lake Helper
Handles S3 operations for storing JDE data as Parquet files
"""
import boto3
import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from env_loader import get_env_var
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

class S3DataLakeHelper:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=get_env_var('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=get_env_var('AWS_SECRET_ACCESS_KEY'),
            region_name=get_env_var('AWS_REGION', 'us-east-1')
        )
        self.bucket_name = get_env_var('S3_BUCKET_NAME', 'bakery-operations-data-lake')
        self.base_prefix = get_env_var('S3_BASE_PREFIX', 'jde-ingestion')
    
    def store_jde_dispatch(self, data: List[Dict], dispatch_type: str, transaction_date: str = None) -> str:
        """
        Store JDE dispatch data as Parquet file in S3
        
        Args:
            data: List of dictionaries containing the dispatch data
            dispatch_type: Type of dispatch ('to_bakery_ops', 'from_bakery_ops', 'cardex_changes')
            transaction_date: Optional date string, defaults to current date
            
        Returns:
            S3 key where the data was stored
        """
        if not transaction_date:
            transaction_date = datetime.now().strftime('%Y-%m-%d')
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f"{self.base_prefix}/{dispatch_type}/year={transaction_date[:4]}/month={transaction_date[5:7]}/day={transaction_date[8:10]}/dispatch_{timestamp}.parquet"
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Convert DataFrame to Parquet bytes
            buffer = BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream',
                Metadata={
                    'dispatch_type': dispatch_type,
                    'transaction_date': transaction_date,
                    'record_count': str(len(data)),
                    'created_at': datetime.now().isoformat()
                }
            )
            
            logger.info(f"Successfully stored {len(data)} records to S3: s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Failed to store data to S3: {str(e)}")
            raise
    
    def get_dispatch_data(self, s3_key: str) -> pd.DataFrame:
        """
        Retrieve dispatch data from S3 Parquet file
        
        Args:
            s3_key: S3 key of the Parquet file
            
        Returns:
            DataFrame containing the dispatch data
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            parquet_bytes = response['Body'].read()
            
            # Convert bytes to DataFrame
            buffer = BytesIO(parquet_bytes)
            df = pd.read_parquet(buffer, engine='pyarrow')
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to retrieve data from S3: {str(e)}")
            raise
    
    def list_dispatches(self, dispatch_type: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """
        List available dispatch files in S3
        
        Args:
            dispatch_type: Filter by dispatch type
            start_date: Filter by start date (YYYY-MM-DD)
            end_date: Filter by end date (YYYY-MM-DD)
            
        Returns:
            List of dispatch file metadata
        """
        prefix = self.base_prefix
        if dispatch_type:
            prefix = f"{self.base_prefix}/{dispatch_type}"
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            dispatches = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith('.parquet'):
                            # Extract metadata from key path
                            parts = key.split('/')
                            if len(parts) >= 6:
                                dispatch_type_from_key = parts[1]
                                year = parts[2].replace('year=', '')
                                month = parts[3].replace('month=', '')
                                day = parts[4].replace('day=', '')
                                file_date = f"{year}-{month}-{day}"
                                
                                # Apply date filters if provided
                                if start_date and file_date < start_date:
                                    continue
                                if end_date and file_date > end_date:
                                    continue
                                
                                dispatches.append({
                                    'key': key,
                                    'dispatch_type': dispatch_type_from_key,
                                    'date': file_date,
                                    'size': obj['Size'],
                                    'last_modified': obj['LastModified'].isoformat()
                                })
            
            return sorted(dispatches, key=lambda x: x['last_modified'], reverse=True)
            
        except Exception as e:
            logger.error(f"Failed to list dispatches: {str(e)}")
            raise
    
    def store_schema(self, table_name: str, schema: Dict) -> str:
        """
        Store table schema in S3 for reference
        
        Args:
            table_name: Name of the table/entity
            schema: Schema definition as dictionary
            
        Returns:
            S3 key where the schema was stored
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f"{self.base_prefix}/schemas/{table_name}/schema_{timestamp}.json"
        
        try:
            schema_with_metadata = {
                'table_name': table_name,
                'schema': schema,
                'created_at': datetime.now().isoformat(),
                'version': timestamp
            }
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(schema_with_metadata, indent=2),
                ContentType='application/json',
                Metadata={
                    'table_name': table_name,
                    'created_at': datetime.now().isoformat()
                }
            )
            
            logger.info(f"Successfully stored schema for {table_name} to S3: s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Failed to store schema to S3: {str(e)}")
            raise
    
    def get_latest_schema(self, table_name: str) -> Optional[Dict]:
        """
        Get the latest schema for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Schema dictionary or None if not found
        """
        prefix = f"{self.base_prefix}/schemas/{table_name}/"
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return None
            
            # Get the most recent schema file
            latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
            
            obj_response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=latest_file['Key']
            )
            
            schema_data = json.loads(obj_response['Body'].read().decode('utf-8'))
            return schema_data['schema']
            
        except Exception as e:
            logger.error(f"Failed to retrieve schema from S3: {str(e)}")
            return None

# Global instance
s3_helper = S3DataLakeHelper()
