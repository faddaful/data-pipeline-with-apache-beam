import json
import os
import hashlib
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.gcsio import GcsIO
from google.cloud import pubsub_v1

class DataQualityCheck(beam.DoFn):
    """
    A Beam DoFn to perform data quality checks on each row of the input data.
    """
    
    def process(self, element):
        """
        Process a single CSV row and perform data quality checks.

        Args:
            element (str): A single row from the CSV file as a string.

        Yields:
            tuple: A tuple containing the original element and a list of any data quality issues.
        """
        fields = element.split(",")
        issues = []

        # Check 1: Ensure correct number of fields
        if len(fields) != 16:
            issues.append("Incorrect number of fields")

        # Check 2: Validate price (should be a positive integer)
        try:
            price = int(fields[1].strip('"'))
            if price <= 0:
                issues.append("Invalid price (non-positive)")
        except ValueError:
            issues.append("Invalid price (not an integer)")

        # Check 3: Validate date format (assuming YYYY-MM-DD)
        date = fields[2]
        if not (len(date) == 10 and date[4] == '-' and date[7] == '-'):
            issues.append("Invalid date format")

        # Check 4: Ensure postcode is not empty
        if not fields[3].strip():
            issues.append("Missing postcode")

        # Yield the original element along with any issues found
        yield (element, issues)

class ParseCSV(beam.DoFn):
    """
    A Beam DoFn (function) to parse CSV rows and generate a property transaction record.

    This class processes each row from the CSV file, extracting fields and generating 
    a unique property ID for each row. The result is a tuple containing the property ID 
    and the associated transaction details.

    Methods:
        process: Processes a single CSV row, converts it into a transaction dictionary.
        generate_property_id: Creates a unique property ID based on property address fields.
    """
    
    def process(self, element):
        """
        Process a single CSV row into a transaction dictionary and yield the result.

        Args:
            element (str): A single row from the CSV file as a string.

        Yields:
            tuple: A tuple containing a unique property ID and the transaction data.

        Raises:
            Prints an error message if any exception occurs during processing.
        """
        try:
            # Split the CSV row into individual fields
            fields = element.split(",")
            
            # Generate a unique property ID using selected fields
            property_id = self.generate_property_id(fields)
            
            # Create a dictionary representing the transaction details
            transaction = {
                "transaction_id": fields[0],
                "price": int(fields[1].strip('"')),  # Remove quotes and convert to integer
                "date": fields[2],
                "postcode": fields[3],
                "property_type": fields[4],
                "old_new": fields[5],
                "duration": fields[6],
                "paon": fields[7],
                "saon": fields[8],
                "street": fields[9],
                "locality": fields[10],
                "town_city": fields[11],
                "district": fields[12],
                "county": fields[13],
                "ppd_category_type": fields[14],
                "record_status": fields[15]
            }
            
            # Yield the property ID and transaction data as a tuple
            yield (property_id, transaction)
        
        except Exception as e:
            # Handle any errors that occur during processing
            print(f"Error processing row: {element}")
            print(f"Error details: {str(e)}")

    @staticmethod
    def generate_property_id(fields):
        """
        Generate a unique property ID using selected address fields.

        The property ID is created by concatenating several address fields (PAON, SAON, street, 
        and postcode), removing spaces, converting to lowercase, and then applying MD5 hashing.

        Args:
            fields (list): List of fields from a CSV row.

        Returns:
            str: MD5 hash as the unique property ID.
        """
        # Concatenate address-related fields, remove spaces, and convert to lowercase
        address = f"{fields[7]}{fields[8]}{fields[9]}{fields[3]}".lower().replace(" ", "")
        
        # Return MD5 hash of the concatenated address string
        return hashlib.md5(address.encode()).hexdigest()


class GroupTransactionsByProperty(beam.DoFn):
    """
    A Beam DoFn to group transactions by property and convert them into JSON format.

    This class processes the grouped property transactions, sorts them by date, and formats
    them as a JSON object that can be output to a file or database.

    Methods:
        process: Processes grouped transactions and yields them as JSON strings.
    """
    
    def process(self, element):
        """
        Process a group of transactions for a single property and yield them as a JSON string.

        Args:
            element (tuple): A tuple containing the property ID and a list of transactions.

        Yields:
            str: A JSON-formatted string representing the property and its transactions.
        """
        property_id, transactions = element
        
        # Create a dictionary representing the property and its sorted transactions
        property_data = {
            "property_id": property_id,
            "transactions": sorted(transactions, key=lambda x: x["date"])  # Sort by transaction date
        }
        
        # Yield the JSON string of the property data
        yield json.dumps(property_data)

def run_pipeline(input_file, output_file):
    """
    Defines and runs the Apache Beam pipeline to process property transaction data.

    The pipeline reads a CSV file, performs data quality checks, parses each row to generate 
    transaction data, groups transactions by property, and outputs the result as JSON.

    Args:
        input_file (str): Path to the input CSV file.
        output_file (str): Path to save the output JSON data.
    """
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as p:
        # Read and perform data quality checks
        data_with_quality_checks = (p
            | "Read CSV" >> ReadFromText(input_file, skip_header_lines=1)
            | "Data Quality Check" >> beam.ParDo(DataQualityCheck())
        )

        # Filter out rows with data quality issues
        clean_data = data_with_quality_checks | "Filter Clean Data" >> beam.Filter(lambda x: not x[1])

        # Process clean data
        output = (clean_data
            | "Extract Clean Data" >> beam.Map(lambda x: x[0])
            | "Parse CSV" >> beam.ParDo(ParseCSV())
            | "Group by Property" >> beam.GroupByKey()
            | "Format JSON" >> beam.ParDo(GroupTransactionsByProperty())
        )

        # Write the output to a local JSON file
        output | "Write to Local File" >> beam.io.WriteToText(output_file, file_name_suffix='.json')

        # Write data quality issues to a separate file
        issues = (data_with_quality_checks
            | "Filter Issues" >> beam.Filter(lambda x: x[1])
            | "Format Issues" >> beam.Map(lambda x: json.dumps({"row": x[0], "issues": x[1]}))
            | "Write Issues" >> beam.io.WriteToText(output_file + "_issues", file_name_suffix='.json')
        )

        # # For the future: Write to GCS (Google Cloud Storage)
        # output | "Write to GCS" >> beam.io.WriteToText(f"gs://{land_registry_bucket}/input/data.json")

        # # Optional: Publish a message to Pub/Sub after processing
        # publisher = pubsub_v1.PublisherClient()
        # topic_path = publisher.topic_path('land-registry-project-id', 'land-registry-updates')
        # publisher.publish(topic_path, json.dumps({
        #     'input_path': f"gs://{land_registry_bucket}/input/data.json",
        #     'output_table': output_table
        # }).encode('utf-8')

if __name__ == "__main__":
    """
    Entry point for running the pipeline. It sets input/output file paths and ensures
    the output directory exists before running the pipeline.
    """
    # input_file = "gs://land-registry-bucket/land-registry-data.csv"
    # output_bucket = "land-registyry-project-id-processed-data"
    # output_table = "land-registry-project:land-registry_dataset.land_registry_transactions"
    # run_pipeline(input_file, output_bucket, output_table)
    
    # Set up relative paths for input and output files
    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(current_dir, "fetch-data-2014-2024.csv")  # Input CSV file
    output_file = os.path.join(current_dir, "output", "transformed_data")  # Output directory for JSON data
    
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Run the Apache Beam pipeline with the provided input and output paths
    run_pipeline(input_file, output_file)

    # Notification that the pipeline has completed and show the output file location
    print(f"Pipeline completed. Output saved to {output_file}-00000-of-00001.json")
    print(f"Data quality issues saved to {output_file}_issues-00000-of-00001.json")