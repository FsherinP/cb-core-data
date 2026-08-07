#!/usr/bin/env python3
"""
Script to read PostgreSQL table and write to Parquet format.
Reads the org_hierarchy_new table from PostgreSQL database and exports it as a Parquet file.
"""

import psycopg2
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import sys
import logging
from typing import Optional
import argparse
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from jobs.config import get_environment_config
from jobs.default_config import create_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

config_dict = get_environment_config()
config = create_config(config_dict)


class PostgresToParquetExporter:
    """Export PostgreSQL table to Parquet format."""

    def __init__(self,output_dir: str = './output'):
        """
        Initialize the exporter with database connection parameters.

        Args:
            dbname: PostgreSQL database name
            user: Database username
            password: Database password
            host: Database host address
            port: Database port
            output_dir: Directory to save the Parquet file
        """
        host, port = config.dwPostgresHost.split(":")
        self.connection_params = {
            'dbname': config.appPostgresSchema,
            'user': config.appPostgresUsername,
            'password': config.appPostgresCredential,
            'host': host,
            'port': port
        }
        self.output_dir = output_dir
        self.connection = None

        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)

    def connect(self) -> None:
        """Establish connection to PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            logger.info(f"Successfully connected to PostgreSQL database: {self.connection_params['dbname']}")
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def disconnect(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

    def read_table(self, table_name: str = 'org_hierarchy_new', 
                   schema: str = 'public',
                   chunk_size: Optional[int] = None) -> pd.DataFrame:
        """
        Read table from PostgreSQL database.

        Args:
            table_name: Name of the table to read
            schema: Schema name
            chunk_size: If specified, reads data in chunks (useful for large tables)

        Returns:
            DataFrame containing the table data
        """
        if not self.connection:
            raise ConnectionError("Database connection not established. Call connect() first.")

        query = f"SELECT * FROM {schema}.{table_name}"

        try:
            logger.info(f"Reading table: {schema}.{table_name}")

            if chunk_size:
                # Read in chunks for large tables
                chunks = []
                for chunk in pd.read_sql_query(query, self.connection, chunksize=chunk_size):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
                logger.info(f"Read {len(df)} rows in chunks of {chunk_size}")
            else:
                # Read entire table at once
                df = pd.read_sql_query(query, self.connection)
                logger.info(f"Read {len(df)} rows from table")

            return df

        except Exception as e:
            logger.error(f"Failed to read table: {e}")
            raise

    def write_to_parquet(self, 
                        df: pd.DataFrame, 
                        filename: Optional[str] = None,
                        compression: str = 'snappy',
                        use_deprecated_int96_timestamps: bool = False) -> str:
        """
        Write DataFrame to Parquet file.

        Args:
            df: DataFrame to write
            filename: Output filename (without extension). If None, generates timestamp-based name
            compression: Compression algorithm ('snappy', 'gzip', 'brotli', 'lz4', 'zstd', or None)
            use_deprecated_int96_timestamps: Use INT96 timestamps for compatibility

        Returns:
            Path to the written Parquet file
        """
        if filename is None:
            filename = f"org_hierarchy_new"

        output_path = os.path.join(self.output_dir, f"{filename}.parquet")

        try:
            logger.info(f"Writing {len(df)} rows to Parquet file: {output_path}")

            # Configure Parquet write options
            table = pa.Table.from_pandas(df)

            pq.write_table(
                table,
                output_path,
                compression=compression,
                use_deprecated_int96_timestamps=use_deprecated_int96_timestamps
            )

            # Get file size
            file_size = os.path.getsize(output_path) / (1024 * 1024)  # Convert to MB
            logger.info(f"Successfully wrote Parquet file: {output_path} (Size: {file_size:.2f} MB)")

            return output_path

        except Exception as e:
            logger.error(f"Failed to write Parquet file: {e}")
            raise

    def export_table_to_parquet(self, 
                               table_name: str = 'org_hierarchy_new',
                               schema: str = 'public',
                               filename: Optional[str] = None,
                               chunk_size: Optional[int] = None,
                               compression: str = 'snappy') -> str:
        """
        Complete pipeline to export PostgreSQL table to Parquet.

        Args:
            table_name: Name of the table to export
            schema: Database schema
            filename: Output filename
            chunk_size: Chunk size for reading large tables
            compression: Parquet compression algorithm

        Returns:
            Path to the exported Parquet file
        """
        try:
            # Connect to database
            self.connect()

            # Read table
            df = self.read_table(table_name, schema, chunk_size)

            # Display basic statistics
            logger.info(f"Table shape: {df.shape}")
            logger.info(f"Columns: {df.columns.tolist()}")
            logger.info(f"Data types:\n{df.dtypes}")

            # Check for null values
            null_counts = df.isnull().sum()
            if null_counts.any():
                logger.info(f"Null values per column:\n{null_counts[null_counts > 0]}")

            # Write to Parquet
            output_path = self.write_to_parquet(df, filename, compression)

            return output_path

        finally:
            # Always disconnect
            self.disconnect()


def main():
    """Main function with command-line interface."""
    parser = argparse.ArgumentParser(description='Export PostgreSQL table to Parquet format')

    # Export arguments
    parser.add_argument('--table', default='org_hierarchy_new', help='Table name to export')
    parser.add_argument('--schema', default='public', help='Database schema')
    parser.add_argument('--output-dir', default='./output', help='Output directory for Parquet files')
    parser.add_argument('--filename', help='Output filename (without extension)')
    parser.add_argument('--chunk-size', type=int, help='Chunk size for reading large tables')
    parser.add_argument('--compression', default='snappy', 
                       choices=['snappy', 'gzip', 'brotli', 'lz4', 'zstd', 'none'],
                       help='Parquet compression algorithm')

    args = parser.parse_args()

    # Handle 'none' compression
    compression = None if args.compression == 'none' else args.compression

    try:
        # Create exporter instance
        exporter = PostgresToParquetExporter(
            output_dir=args.output_dir
        )

        # Export table to Parquet
        output_path = exporter.export_table_to_parquet(
            table_name=args.table,
            schema=args.schema,
            filename=args.filename,
            chunk_size=args.chunk_size,
            compression=compression
        )

        logger.info(f"Export completed successfully! File saved to: {output_path}")
        return 0

    except Exception as e:
        logger.error(f"Export failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
