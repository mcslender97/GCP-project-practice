#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A topic"""

# pytype: skip-file

# beam-playground:

#   multifile: false
#   pipeline_options: --input inputfile.json --topic topic_name


import argparse
import logging


import pandas as pd
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
import apache_beam.io.gcp.pubsub as pbs

class ConvertJSONToCSV(beam.DoFn):
 
  def process(self, element):
    df=pd.DataFrame(pd.read_json(element))
    return df.to_csv()




def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://yelp-data-proj-bucket/yelp_academic_dataset_business.json',
      help='Input file to process.')
  parser.add_argument(
      '--topic',
      dest='topic',
      default='projects/yelp--data-eng/topics/yelp-topic',
      help='Pubsub Topic to write to')
  known_args, pipeline_args = parser.parse_known_args(argv)


  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  
  pipeline_options.view_as(StandardOptions).streaming = True

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    input_file = p | 'Read' >> ReadFromText(known_args.input)
    
    output = input_file | 'ConvertToCSVFormat' >> (beam.ParDo(ConvertJSONToCSV()).with_output_types(str))
    output | 'Write' >> pbs.WriteToPubSub(known_args.topic)
    # input_file | 'Write' >> pbs.WriteToPubSub(known_args.topic)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()