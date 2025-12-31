import pytest 
import pandas as pd
import datetime
from src.ecommerce_etl import transforms


def test_flagged_df(sample_df_to_flag):
    #Arrange to get a flagged df
    enriched_df = transforms.flag_df(sample_df_to_flag)

    # Assert: Verify secific cases by using Pandas filters
    # Case 1: Quantity 0 -> Anomaly 
    assert len(enriched_df[enriched_df['Flag'] == "Anomaly"]) == 2
    
    # Case 2: Negative quantity and price > 0 -> Refunds
    assert len(enriched_df[enriched_df['Flag'] == "Refund"]) == 1
    
    # Case 3: Price 0 and quatity > 0 -> Promotion
    assert len(enriched_df[enriched_df['Flag'] == "Promotion/Gift"]) == 1

    #Case 4: Regular sell (Price >0 & Quantity >0) -> Standard
    assert len(enriched_df[enriched_df['Flag'] == "Standard"]) == 1

    #Case non null in the new column
    assert enriched_df['Flag'].isna().sum() == 0




""" def test_transaction_with_negative_quatity_raises_business_error():
    pass
    qué pasa con precio y cantidad cero?

def test_transaction_with_negative_unit_price_raises_business_error(sample_invalid_df):
    with pytest.raises(source_validator.UnitPriceError):
        source_validator.validate_unit_price(sample_invalid_df) """