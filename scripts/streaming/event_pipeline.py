# scripts/streaming/event_pipeline.py
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
            event = json.loads(element.decode('utf-8'))
            yield event
        except Exception as e:
            logging.error(f"Error parsing event: {e}")
            return []

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_subscription', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--temp_location', required=True)
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    
    with beam.Pipeline(options=options) as p:
        events = (
            p 
            | 'Read from PubSub' >> ReadFromPubSub(subscription=known_args.input_subscription)
            | 'Parse JSON' >> beam.ParDo(ParseJsonDoFn())
            | 'Write to BigQuery' >> WriteToBigQuery(
                known_args.output_table,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()