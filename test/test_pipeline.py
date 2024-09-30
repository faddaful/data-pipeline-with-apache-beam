import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from src.pipeline.beam_pipeline import ParseCSV, GroupTransactionsByProperty
import json

def test_parse_csv():
    with TestPipeline() as p:
        input_data = [
            "1,100000,2021-01-01,AB1 2CD,D,N,F,123,FLAT 1,HIGH STREET,CITY,LONDON,GREATER LONDON,GREATER LONDON,A,A"
        ]
        output = (
            p
            | beam.Create(input_data)
            | beam.ParDo(ParseCSV())
        )
        
        assert_that(output, equal_to([
            (ParseCSV.generate_property_id(input_data[0].split(",")), {
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
    with TestPipeline() as p:
        input_data = [
            ("property1", [
                {"transaction_id": "1", "date": "2021-01-01", "price": 100000},
                {"transaction_id": "2", "date": "2022-01-01", "price": 110000}
            ])
        ]
        output = (
            p
            | beam.Create(input_data)
            | beam.ParDo(GroupTransactionsByProperty())
        )
        
        expected_output = [
            json.dumps({
                "property_id": "property1",
                "transactions": [
                    {"transaction_id": "1", "date": "2021-01-01", "price": 100000},
                    {"transaction_id": "2", "date": "2022-01-01", "price": 110000}
                ]
            })
        ]
        
        assert_that(output, equal_to(expected_output))

def test_end_to_end_pipeline():
    with TestPipeline() as p:
        input_data = [
            "1,100000,2021-01-01,AB1 2CD,D,N,F,123,FLAT 1,HIGH STREET,CITY,LONDON,GREATER LONDON,GREATER LONDON,A,A",
            "2,110000,2022-01-01,AB1 2CD,D,N,F,123,FLAT 1,HIGH STREET,CITY,LONDON,GREATER LONDON,GREATER LONDON,A,A"
        ]
        
        output = (
            p
            | beam.Create(input_data)
            | beam.ParDo(ParseCSV())
            | beam.GroupByKey()
            | beam.ParDo(GroupTransactionsByProperty())
        )
        
        property_id = ParseCSV.generate_property_id(input_data[0].split(","))
        expected_output = [
            json.dumps({
                "property_id": property_id,
                "transactions": [
                    {"transaction_id": "1", "price": 100000, "date": "2021-01-01", "postcode": "AB1 2CD", "property_type": "D", "old_new": "N", "duration": "F", "paon": "123", "saon": "FLAT 1", "street": "HIGH STREET", "locality": "CITY", "town_city": "LONDON", "district": "GREATER LONDON", "county": "GREATER LONDON", "ppd_category_type": "A", "record_status": "A"},
                    {"transaction_id": "2", "price": 110000, "date": "2022-01-01", "postcode": "AB1 2CD", "property_type": "D", "old_new": "N", "duration": "F", "paon": "123", "saon": "FLAT 1", "street": "HIGH STREET", "locality": "CITY", "town_city": "LONDON", "district": "GREATER LONDON", "county": "GREATER LONDON", "ppd_category_type": "A", "record_status": "A"}
                ]
            })
        ]
        
        assert_that(output, equal_to(expected_output))