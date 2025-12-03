"""
Data enrichment module for Dubai Real Estate.

Enriches raw rent contract data with calculated fields, classifications,
and temporal features to enable better analysis.
"""

import logging
from datetime import datetime
import polars as pl

from lib.config import (
    get_area_tier,
    normalize_property_type,
    is_residential,
    is_commercial,
    MARKET_METRICS,
)

logger = logging.getLogger(__name__)


class RentContractsEnricher:
    """
    Enriches rent contract data with calculated fields and classifications.
    
    Features:
    - Price per square foot calculation
    - Area tier classification
    - Property type normalization
    - Temporal features (quarter, season, year)
    - Contract duration calculation
    - Luxury property flagging
    """
    
    def __init__(self, data: pl.DataFrame):
        """
        Initialize enricher.
        
        Args:
            data: DataFrame containing rent contract data
        """
        self.data = data
        
    def enrich(self) -> pl.DataFrame:
        """
        Apply all enrichment transformations.
        
        Returns:
            Enriched DataFrame
        """
        logger.info("Starting data enrichment...")
        
        enriched = self.data
        
        # Calculate PSF
        enriched = self._add_psf(enriched)
        
        # Add area tier classification
        enriched = self._add_area_tier(enriched)
        
        # Normalize property types
        enriched = self._normalize_property_types(enriched)
        
        # Add temporal features
        enriched = self._add_temporal_features(enriched)
        
        # Calculate contract duration
        enriched = self._add_contract_duration(enriched)
        
        # Flag luxury properties
        enriched = self._flag_luxury_properties(enriched)
        
        # Add usage category
        enriched = self._add_usage_category(enriched)
        
        logger.info(f"Enrichment complete. Added {len(enriched.columns) - len(self.data.columns)} new columns")
        
        return enriched
        
    def _add_psf(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add price per square foot calculation."""
        if "actual_area" in df.columns and "annual_amount" in df.columns:
            logger.debug("Adding PSF calculation...")
            
            df = df.with_columns(
                pl.when(
                    (pl.col("actual_area").is_not_null()) &
                    (pl.col("actual_area") > 0) &
                    (pl.col("annual_amount").is_not_null()) &
                    (pl.col("annual_amount") > 0)
                )
                .then(pl.col("annual_amount") / pl.col("actual_area"))
                .otherwise(None)
                .alias("price_per_sqft")
            )
        
        return df
        
    def _add_area_tier(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add area tier classification."""
        if "area_name_en" in df.columns:
            logger.debug("Adding area tier classification...")
            
            # This is a simplified version - in production, you'd use a mapping
            # For now, we'll add a placeholder that can be updated with actual mappings
            df = df.with_columns(
                pl.lit("Mid-Tier").alias("area_tier")
            )
            
            # TODO: Implement actual area tier mapping from config
            # df = df.with_columns(
            #     pl.col("area_en").map_dict(AREA_TIER_MAPPING).alias("area_tier")
            # )
        
        return df
        
    def _normalize_property_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize property type names."""
        if "ejari_property_type_en" in df.columns:
            logger.debug("Normalizing property types...")
            
            # Add normalized property type column
            df = df.with_columns(
                pl.col("ejari_property_type_en")
                .str.to_lowercase()
                .str.strip_chars()
                .alias("property_type_normalized")
            )
        
        return df
        
    def _add_temporal_features(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add temporal features from contract start date."""
        if "contract_start_date" in df.columns:
            logger.debug("Adding temporal features...")
            
            df = df.with_columns([
                pl.col("contract_start_date").dt.year().alias("contract_year"),
                pl.col("contract_start_date").dt.quarter().alias("contract_quarter"),
                pl.col("contract_start_date").dt.month().alias("contract_month"),
                pl.col("contract_start_date").dt.weekday().alias("contract_weekday"),
            ])
            
            # Add season
            df = df.with_columns(
                pl.when(pl.col("contract_month").is_in([12, 1, 2]))
                .then(pl.lit("Winter"))
                .when(pl.col("contract_month").is_in([3, 4, 5]))
                .then(pl.lit("Spring"))
                .when(pl.col("contract_month").is_in([6, 7, 8]))
                .then(pl.lit("Summer"))
                .otherwise(pl.lit("Fall"))
                .alias("contract_season")
            )
        
        return df
        
    def _add_contract_duration(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate contract duration in days."""
        if "contract_start_date" in df.columns and "contract_end_date" in df.columns:
            logger.debug("Calculating contract duration...")
            
            df = df.with_columns(
                pl.when(
                    (pl.col("contract_start_date").is_not_null()) &
                    (pl.col("contract_end_date").is_not_null())
                )
                .then((pl.col("contract_end_date") - pl.col("contract_start_date")).dt.total_days())
                .otherwise(None)
                .alias("contract_duration_days")
            )
            
            # Add duration category
            df = df.with_columns(
                pl.when(pl.col("contract_duration_days") < 180)
                .then(pl.lit("Short-term"))
                .when(pl.col("contract_duration_days") < 365)
                .then(pl.lit("Medium-term"))
                .when(pl.col("contract_duration_days") >= 365)
                .then(pl.lit("Long-term"))
                .otherwise(pl.lit("Unknown"))
                .alias("contract_duration_category")
            )
        
        return df
        
    def _flag_luxury_properties(self, df: pl.DataFrame) -> pl.DataFrame:
        """Flag luxury properties based on rent and PSF."""
        if "price_per_sqft" in df.columns and "annual_amount" in df.columns:
            logger.debug("Flagging luxury properties...")
            
            # Calculate percentile thresholds
            valid_data = df.filter(
                (pl.col("price_per_sqft").is_not_null()) &
                (pl.col("annual_amount").is_not_null())
            )
            
            if valid_data.height > 0:
                psf_threshold = valid_data["price_per_sqft"].quantile(
                    MARKET_METRICS["luxury_psf_percentile"] / 100
                )
                rent_threshold = valid_data["annual_amount"].quantile(
                    MARKET_METRICS["luxury_rent_percentile"] / 100
                )
                
                df = df.with_columns(
                    pl.when(
                        (pl.col("price_per_sqft") >= psf_threshold) |
                        (pl.col("annual_amount") >= rent_threshold)
                    )
                    .then(pl.lit(True))
                    .otherwise(pl.lit(False))
                    .alias("is_luxury")
                )
            else:
                df = df.with_columns(pl.lit(False).alias("is_luxury"))
        
        return df
        
    def _add_usage_category(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add simplified usage category (Residential/Commercial/Other)."""
        if "property_usage_en" in df.columns:
            logger.debug("Adding usage category...")
            
            df = df.with_columns(
                pl.when(pl.col("property_usage_en").str.contains("(?i)residential"))
                .then(pl.lit("Residential"))
                .when(pl.col("property_usage_en").str.contains("(?i)commercial"))
                .then(pl.lit("Commercial"))
                .otherwise(pl.lit("Other"))
                .alias("usage_category")
            )
        
        return df


def enrich_rent_contracts(df: pl.DataFrame) -> pl.DataFrame:
    """
    Convenience function to enrich rent contracts data.
    
    Args:
        df: DataFrame to enrich
        
    Returns:
        Enriched DataFrame
    """
    enricher = RentContractsEnricher(df)
    return enricher.enrich()
