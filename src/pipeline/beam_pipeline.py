
import json
import os
import hashlib
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.gcsio import GcsIO
from google.cloud import pubsub_v1


class ParseCSV(beam.DoFn):
    def process(self, element):
        try:
            fields = element.split(",")
            property_id = self.generate_property_id(fields)
            transaction = {
                "transaction_id": fields[0],
                "price": int(fields[1].strip('"')),
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
            yield (property_id, transaction)
        except Exception as e:
            print(f"Error processing row: {element}")
            print(f"Error details: {str(e)}")
        

    @staticmethod
    def generate_property_id(fields):
        address = f"{fields[7]}{fields[8]}{fields[9]}{fields[3]}".lower().replace(" ", "")
        return hashlib.md5(address.encode()).hexdigest()

class GroupTransactionsByProperty(beam.DoFn):
    def process(self, element):
        property_id, transactions = element
        property_data = {
            "property_id": property_id,
            "transactions": sorted(transactions, key=lambda x: x["date"])
        }
        yield json.dumps(property_data)

def run_pipeline(input_file, output_file):
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as p:
        output = (p
         | "Read CSV" >> ReadFromText(input_file, skip_header_lines=1)
         | "Parse CSV" >> beam.ParDo(ParseCSV())
         | "Group by Property" >> beam.GroupByKey()
         | "Format JSON" >> beam.ParDo(GroupTransactionsByProperty())
        )

        # Write to local file
        output | "Write to Local File" >> beam.io.WriteToText(output_file, file_name_suffix='.json')

        # # Write to GCS
        # output | "Write to GCS" >> beam.io.WriteToText(f"gs://{land_registry_bucket}/input/data.json")

        # # Publish message to Pub/Sub
        # publisher = pubsub_v1.PublisherClient()
        # topic_path = publisher.topic_path('land-registry-project-id', 'land-registry-updates')
        # publisher.publish(topic_path, json.dumps({
        #     'input_path': f"gs://{land_registry_bucket}/input/data.json",
        #     'output_table': output_table
        # }).encode('utf-8'))

if __name__ == "__main__":
    # input_file = "gs://land-registry-bucket/land-registry-data.csv"
    # output_bucket = "land-registyry-project-id-processed-data"
    # output_table = "land-registry-project:land-registry_dataset.land_registry_transactions"
    # run_pipeline(input_file, output_bucket, output_table)

    # Use relative paths for local files
    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(current_dir, "fetch-data-2014-2024.csv")
    output_file = os.path.join(current_dir, "output", "transformed_data")
    
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    run_pipeline(input_file, output_file)

print(f"Pipeline completed. Output saved to {output_file}-00000-of-00001.json")