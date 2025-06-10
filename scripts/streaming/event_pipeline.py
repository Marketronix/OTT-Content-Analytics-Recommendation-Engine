# Updated event_pipeline.py
import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery

class ParseJsonDoFn(beam.DoFn):
    """Parse JSON messages from Pub/Sub."""
    def process(self, element):
        try:
            # Parse JSON message
            event = json.loads(element.decode('utf-8'))
            logging.info(f"Parsed event: {event['event_id']}")
            yield event
        except Exception as e:
            logging.error(f"Error parsing event: {str(e)}")
            return []

def run(argv=None):
    parser = argparse.ArgumentParser(description='Process streaming events')
    parser.add_argument('--input_subscription', required=True, 
                      help='Pub/Sub subscription to read from')
    parser.add_argument('--output_table', required=True, 
                      help='BigQuery table to write to')
    parser.add_argument('--temp_location', required=True, 
                      help='GCS location for temporary files')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    logging.info(f"Starting pipeline with input_subscription={known_args.input_subscription}")
    logging.info(f"Output table: {known_args.output_table}")
    logging.info(f"Temp location: {known_args.temp_location}")
    
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    
    # Define the BigQuery schema
    schema = {
        'fields': [
            {'name': 'event_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'user_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'content_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'event_type', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
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
        ]
    }
    
    with beam.Pipeline(options=pipeline_options) as p:
        events = (
            p 
            | 'Read from PubSub' >> ReadFromPubSub(
                subscription=known_args.input_subscription)
            | 'Parse JSON' >> beam.ParDo(ParseJsonDoFn())
            | 'Write to BigQuery' >> WriteToBigQuery(
                known_args.output_table,
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()