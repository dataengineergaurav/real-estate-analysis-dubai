"""
Rent contracts transformer with data quality checks and validation.
"""

from datetime import date
import logging
import polars as pl
from typing import Optional

from lib.config import FILE_CONFIG
from lib.classes.validators import validate_rent_contracts

logger = logging.getLogger(__name__)


class RentContractsTransformer:
    """
    Transforms rent contract CSV data to optimized Parquet format.
    
    Features:
    - Data quality validation
    - Schema enforcement
    - Transformation statistics
    - Comprehensive error handling
    """
    
    def __init__(self, input_file: str, output_file: str, validate: bool = True):
        """
        Initialize transformer.
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output Parquet file
            validate: Whether to run data validation
        """
        self.input_file = input_file
        self.output_file = output_file
        self.validate = validate

    def transform(self) -> bool:
        """
        Transform CSV to Parquet with validation and quality checks.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Starting transformation: {self.input_file} -> {self.output_file}")
            
            # Read the CSV as a LazyFrame with schema overrides
            logger.info("Reading CSV file...")
            lf = pl.scan_csv(
                self.input_file,
                null_values=["null", "NULL", ""],
                schema_overrides={
                    "ejari_property_sub_type_id": pl.Int64,
                    "actual_area": pl.Float64,
                    "annual_amount": pl.Float64,
                }
            )
            
            # Collect to DataFrame for validation
            df = lf.collect()
            
            logger.info(f"Loaded {df.height:,} records with {len(df.columns)} columns")
            
            # Run validation if enabled
            if self.validate:
                logger.info("Running data validation...")
                validation_result = validate_rent_contracts(df, strict=False)
                
                logger.info(f"Validation summary: {validation_result.get_summary()}")
                
                if validation_result.errors:
                    logger.warning("Validation errors found:")
                    for error in validation_result.errors[:10]:  # Show first 10 errors
                        logger.warning(f"  - {error}")
                        
                if validation_result.warnings:
                    logger.info("Validation warnings:")
                    for warning in validation_result.warnings[:5]:  # Show first 5 warnings
                        logger.info(f"  - {warning}")
            
            # Log transformation statistics
            self._log_statistics(df)
            
            # Convert back to LazyFrame for efficient writing
            lf = df.lazy()
            
            # Write to Parquet with compression
            logger.info("Writing to Parquet format...")
            compression = FILE_CONFIG["parquet_compression"]
            compression_level = FILE_CONFIG["parquet_compression_level"]
            
            lf.sink_parquet(
                self.output_file,
                compression=compression,
                compression_level=compression_level
            )
            
            logger.info(f"Successfully transformed data to {self.output_file}")
            return True
            
        except FileNotFoundError as e:
            logger.error(f"Input file not found: {e}")
            return False
            
        except pl.exceptions.ComputeError as e:
            logger.error(f"Polars computation error: {e}")
            return False
            
        except Exception as e:
            logger.exception(f"Unexpected error during transformation: {e}")
            return False
    
    def _log_statistics(self, df: pl.DataFrame) -> None:
        """
        Log transformation statistics.
        
        Args:
            df: DataFrame to analyze
        """
        try:
            # Basic statistics
            logger.info("=== Transformation Statistics ===")
            logger.info(f"Total records: {df.height:,}")
            logger.info(f"Total columns: {len(df.columns)}")
            
            # Null counts for key fields
            if "annual_amount" in df.columns:
                null_rent = df["annual_amount"].null_count()
                logger.info(f"Records with null rent: {null_rent:,}")
                
                if null_rent < df.height:
                    stats = df.filter(pl.col("annual_amount").is_not_null()).select([
                        pl.col("annual_amount").min().alias("min_rent"),
                        pl.col("annual_amount").max().alias("max_rent"),
                        pl.col("annual_amount").mean().alias("avg_rent"),
                        pl.col("annual_amount").median().alias("median_rent"),
                    ]).row(0)
                    
                    logger.info(f"Rent range: AED {stats[0]:,.0f} - {stats[1]:,.0f}")
                    logger.info(f"Average rent: AED {stats[2]:,.0f}")
                    logger.info(f"Median rent: AED {stats[3]:,.0f}")
            
            # Property usage distribution
            if "property_usage_en" in df.columns:
                usage_counts = df.group_by("property_usage_en").agg(
                    pl.len().alias("count")
                ).sort("count", descending=True).limit(5)
                
                logger.info("Top 5 property usage types:")
                for row in usage_counts.iter_rows():
                    logger.info(f"  {row[0]}: {row[1]:,} records")
            
            logger.info("=================================")
            
        except Exception as e:
            logger.warning(f"Error logging statistics: {e}")

