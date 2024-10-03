"""
Test module for FastAPI application to verify the pipeline deployment.
"""
import warnings
import pytest
import json
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from src.pipeline.beam_pipeline import ParseCSV, GroupTransactionsByProperty

# Suppress specific warnings during the test run
warnings.filterwarnings("ignore", category=pytest.PytestWarning)

def test_parse_csv():
    """
    Test case to verify that the ParseCSV DoFn correctly parses a single CSV line.
    
    - Simulates a pipeline that processes a single CSV line and ensures that the
      generated output is in the expected format (a tuple of property ID and transaction).
    """
    with TestPipeline() as p:
        # Input data representing a single CSV row
        input_data = [
            "1,100000,2021-01-01,AB1 2CD,D,N,F,123,FLAT 1,HIGH STREET,CITY,LONDON,GREATER LONDON,GREATER LONDON,A,A"
        ]
        
        # Define the pipeline steps
        output = (
            p
            | beam.Create(input_data)  # Create a PCollection from input data
            | beam.ParDo(ParseCSV())   # Apply the ParseCSV transformation
        )
        
        # Assert that the parsed output matches the expected property ID and transaction
        assert_that(output, equal_to([
            (ParseCSV.generate_property_id(input_data[0].split(",")), {  # Generate property ID
                "transaction_id": "1",
                "price": 100000,
                "date": "2021-01-01",
                "postcode": "AB1 2CD",
                "property_type": "D",
                "old_new": "N",
                "duration": "F",
                "paon": "123",
                "saon": "FLAT 1",
                "street": "HIGH STREET",
                "locality": "CITY",
                "town_city": "LONDON",
                "district": "GREATER LONDON",
                "county": "GREATER LONDON",
                "ppd_category_type": "A",
                "record_status": "A"
            })
        ]))

def test_group_transactions_by_property():
    """
    Test case to verify that GroupTransactionsByProperty DoFn groups transactions by property
    and formats them into a JSON string.

    - Simulates a pipeline that groups multiple transactions for a single property
      and ensures the result is correctly formatted JSON.
    """
    with TestPipeline() as p:
        # Input data containing transactions for a single property
        input_data = [
            ("property1", [
                {"transaction_id": "1", "date": "2021-01-01", "price": 100000},
                {"transaction_id": "2", "date": "2022-01-01", "price": 110000}
            ])
        ]
        
        # Define the pipeline steps
        output = (
            p
            | beam.Create(input_data)  # Create a PCollection from input data
            | beam.ParDo(GroupTransactionsByProperty())  # Apply the grouping and JSON formatting
        )
        
        # Define the expected JSON output
        expected_output = [
            json.dumps({
                "property_id": "property1",
                "transactions": [
                    {"transaction_id": "1", "date": "2021-01-01", "price": 100000},
                    {"transaction_id": "2", "date": "2022-01-01", "price": 110000}
                ]
            })
        ]
        
        # Assert that the output matches the expected JSON
        assert_that(output, equal_to(expected_output))

def test_end_to_end_pipeline():
    """
    End-to-end test case to verify the complete pipeline functionality.
    
    - Simulates an end-to-end run of the pipeline, starting from reading the CSV data,
      parsing it into transactions, grouping by property, and outputting JSON-formatted results.
    """
    with TestPipeline() as p:
        # Input data representing multiple rows from a CSV file
        input_data = [
            "1,100000,2021-01-01,AB1 2CD,D,N,F,123,FLAT 1,HIGH STREET,CITY,LONDON,GREATER LONDON,GREATER LONDON,A,A",
            "2,110000,2022-01-01,AB1 2CD,D,N,F,123,FLAT 1,HIGH STREET,CITY,LONDON,GREATER LONDON,GREATER LONDON,A,A"
        ]
        
        # Define the pipeline steps
        output = (
            p
            | beam.Create(input_data)  # Create a PCollection from input CSV lines
            | beam.ParDo(ParseCSV())   # Parse the CSV lines into transactions
            | beam.GroupByKey()        # Group transactions by property ID
            | beam.ParDo(GroupTransactionsByProperty())  # Format grouped transactions into JSON
        )
        
        # Generate the expected property ID for the input data
        property_id = ParseCSV.generate_property_id(input_data[0].split(","))
        
        # Define the expected JSON output
        expected_output = [
            json.dumps({
                "property_id": property_id,
                "transactions": [
                    {"transaction_id": "1", "price": 100000, "date": "2021-01-01", "postcode": "AB1 2CD", "property_type": "D", "old_new": "N", "duration": "F", "paon": "123", "saon": "FLAT 1", "street": "HIGH STREET", "locality": "CITY", "town_city": "LONDON", "district": "GREATER LONDON", "county": "GREATER LONDON", "ppd_category_type": "A", "record_status": "A"},
                    {"transaction_id": "2", "price": 110000, "date": "2022-01-01", "postcode": "AB1 2CD", "property_type": "D", "old_new": "N", "duration": "F", "paon": "123", "saon": "FLAT 1", "street": "HIGH STREET", "locality": "CITY", "town_city": "LONDON", "district": "GREATER LONDON", "county": "GREATER LONDON", "ppd_category_type": "A", "record_status": "A"}
                ]
            })
        ]
        
        # Assert that the output matches the expected end-to-end JSON result
        assert_that(output, equal_to(expected_output))