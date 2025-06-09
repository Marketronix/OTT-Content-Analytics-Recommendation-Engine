# scripts/streaming/event_pipeline.py
import argparse
import json
import logging
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = os.getenv('BIGQUERY_DATASET')

class ParseJsonDoFn(beam.DoFn):
    """Parse JSON messages from Pub/Sub."""
    def process(self, element):
        try:
            # Parse JSON message
            event = json.loads(element.decode('utf-8'))
            
            # Ensure required fields are present
            required_fields = ['event_id', 'user_id', 'content_id', 'event_type', 'timestamp']
            if not all(field in event for field in required_fields):
                logging.warning(f"Missing required fields in event: {event}")
                return []
            
            # Return the parsed event
            yield event
        except Exception as e:
            logging.error(f"Error parsing event: {e}")
            return []

class EnrichEventsDoFn(beam.DoFn):
    """Enrich events with additional information."""
    def process(self, element):
        try:
            # Add any enrichment logic here
            # For example, you could:
            # - Add content metadata
            # - Add user profile information
            # - Calculate derived fields
            
            # Here we'll just add a processing timestamp
            element['processing_timestamp'] = beam.window.TimestampedValue.get_current_timestamp().to_utc_datetime().isoformat()
            
            yield element
        except Exception as e:
            logging.error(f"Error enriching event: {e}")
            yield element  # Still yield the original element to avoid data loss

class FormatForBigQueryDoFn(beam.DoFn):
    """Format events for BigQuery schema compatibility."""
    def process(self, element):
        try:
            # Ensure nested structures are properly formatted
            # BigQuery expects nested fields as dictionaries
            
            # Handle device field
            if 'device' not in element or not element['device']:
                element['device'] = {
                    'type': None,
                    'os': None,
                    'browser': None,
                    'model': None
                }
            
            # Handle location field
            if 'location' not in element or not element['location']:
                element['location'] = {
                    'country': None,
                    'region': None,
                    'city': None
                }
            
            # Handle quality field
            if 'quality' not in element or not element['quality']:
                element['quality'] = {
                    'resolution': None,
                    'bitrate': None
                }
            
            # Handle timestamps
            if 'timestamp' in element and isinstance(element['timestamp'], str):
                # Ensure timestamp is in BigQuery compatible format
                # This assumes ISO format which is what we're using
                pass
            
            yield element
        except Exception as e:
            logging.error(f"Error formatting event for BigQuery: {e}")
            return []

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=False, default=PROJECT_ID)
    parser.add_argument('--input_subscription', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--runner', default='DataflowRunner')
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--region', required=True)
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Define the pipeline options
    options = PipelineOptions([
        '--project', known_args.project,
        '--runner', known_args.runner,
        '--temp_location', known_args.temp_location,
        '--region', known_args.region,
        '--job_name', 'ott-events-processing',
    ] + pipeline_args)
    
    options.view_as(StandardOptions).streaming = True
    
    # Define the BigQuery table schema
    table_schema = {
        'fields': [
            {'name': 'event_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'user_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'content_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'event_type', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'processing_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'session_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'duration', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'position', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'device', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': [
                {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'os', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'browser', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'model', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]},
            {'name': 'location', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': [
                {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'region', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]},
            {'name': 'quality', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': [
                {'name': 'resolution', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'bitrate', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            ]},
            {'name': 'rating', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'extra_attributes', 'type': 'JSON', 'mode': 'NULLABLE'},
        ]
    }
    
    # Run the pipeline
    with beam.Pipeline(options=options) as p:
        events = (
            p 
            | 'Read from PubSub' >> ReadFromPubSub(subscription=f'projects/{known_args.project}/subscriptions/{known_args.input_subscription}')
            | 'Parse JSON' >> beam.ParDo(ParseJsonDoFn())
            | 'Enrich Events' >> beam.ParDo(EnrichEventsDoFn())
            | 'Format for BigQuery' >> beam.ParDo(FormatForBigQueryDoFn())
            | 'Write to BigQuery' >> WriteToBigQuery(
                known_args.output_table,
                schema=table_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()