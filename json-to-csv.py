# From https://github.com/aFrag/PythonDataflow/blob/master/JsonToCsv.py
import argparse
import json
import logging
import datetime

import apache_beam as beam
import pandas as pd
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from smart_open import open


class ReadJSON(beam.DoFn):

# This pipeline reads JSON file in GCP bucket, convert to csv (pipe-seperated) format and write the new file in GCP
    def process(self, input_path):
        clear_data = []

        dict_obj = json.loads(input_path)
        # clear_data.append(dict_obj.keys())
        for val in dict_obj.values():
            clear_data.append('\"'+(str(val))+'\"') # Add quotes to escape any special character
            # clear_data.append(str(val))
        yield clear_data


class WriteCSVFIle(beam.DoFn):

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def start_bundle(self):
        from google.cloud import storage
        self.client = storage.Client()

    def process(self, mylist):
        bucket = self.client.get_bucket(self.bucket_name)

        bucket.blob(f"csv_exports.csv").upload_from_string(mylist)



        
    


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str, default='gs://yelp-data-proj-bucket/Test.json')
    parser.add_argument('--output', type=str, default='gs://yelp-data-proj-bucket/res.csv')
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session=save_main_session
    def json_csv_map(my_data): # data is given in a list type
        my_data = map(lambda s: s.replace("\n"," "), my_data) # remove any newline character
        
        str_data = '|'.join(my_data) # join each row together using pipe as seperator
        return str_data
        
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (pipeline
         | 'Start' >> ReadFromText(known_args.input_path)
         | 'Read JSON' >> beam.ParDo(ReadJSON())
         | 'Format to CSV' >> beam.Map(json_csv_map)
         | 'Write to File' >> WriteToText(known_args.output)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()