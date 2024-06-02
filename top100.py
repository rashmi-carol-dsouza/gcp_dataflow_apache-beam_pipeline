import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import logging

class FormatResult(beam.DoFn):
    def process(self, element):
        (start_station_id, end_station_id), count = element
        yield f"{start_station_id},{end_station_id},{count}"

def log_element(element):
    logging.info(element)
    return element

def run():
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Define the pipeline options
    options = PipelineOptions()
    gcloud_options = options.view_as(GoogleCloudOptions)
    gcloud_options.project = 'ml-6-coding-challenge-425112'
    gcloud_options.job_name = 'count-rides'
    gcloud_options.staging_location = 'gs://ml6-bucket-11/staging'
    gcloud_options.temp_location = 'gs://ml6-bucket-11/temp'
    gcloud_options.region = 'eu-west1'  # Ensure this matches your dataset region

    with beam.Pipeline(options=options) as p:
        # Read ride data from BigQuery
        rides = (
            p | 'Read Rides Data' >> beam.io.ReadFromBigQuery(
                query='''
                    SELECT start_station_id, end_station_id
                    FROM `bigquery-public-data.london_bicycles.cycle_hire`
                ''',
                use_standard_sql=True
            )
            | 'Log Read Rides' >> beam.Map(log_element)  # Log the read rides data
        )

        # Count the number of rides for each start and end station combination
        ride_counts = (
            rides
            | 'Pair Rides' >> beam.Map(lambda row: ((row['start_station_id'], row['end_station_id']), 1))
            | 'Count Rides' >> beam.CombinePerKey(sum)
        )

        # Get the top 100 ride counts
        top_ride_counts = (
            ride_counts
            | 'To List' >> beam.combiners.Top.Of(100, key=lambda kv: kv[1])
            | 'Flatten' >> beam.FlatMap(lambda elements: elements)
            | 'Format Results' >> beam.ParDo(FormatResult())
        )

        # Write the results to a text file
        top_ride_counts | 'Write to Text' >> beam.io.WriteToText('gs://ml6-bucket-11/output/top_rides_count', shard_name_template='')

if __name__ == '__main__':
    run()
