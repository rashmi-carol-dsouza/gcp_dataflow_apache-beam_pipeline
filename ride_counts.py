import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import logging

class FormatResult(beam.DoFn):
    def process(self, element):
        (start_station_id, end_station_id), count = element
        yield f"{start_station_id},{end_station_id},{count}"

def filter_none_ids(row):
    start_station_id = row['start_station_id']
    end_station_id = row['end_station_id']
    return start_station_id is not None and end_station_id is not None

def run():
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Define the pipeline options
    options = PipelineOptions(
        runner='DataflowRunner',
        project='ml-6-coding-challenge-425112',
        job_name='count-rides',
        staging_location='gs://ml6-challenge-dataflow-bucket/staging',
        temp_location='gs://ml6-challenge-dataflow-bucket/temp',
        region='europe-west1'
    )

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
        )

        # Filter out rows with None values for start_station_id or end_station_id
        filtered_rides = rides | 'Filter None IDs' >> beam.Filter(filter_none_ids)

        # Count the number of rides for each start and end station combination
        ride_counts = (
            filtered_rides
            | 'Pair Rides' >> beam.Map(lambda row: ((row['start_station_id'], row['end_station_id']), 1))
            | 'Count Rides' >> beam.CombinePerKey(sum)
            | 'Format Results' >> beam.ParDo(FormatResult())
        )

        # Write the results to a text file
        ride_counts | 'Write to Text' >> beam.io.WriteToText('gs://ml6-challenge-dataflow-bucket/output/ride_counts', shard_name_template='')

if __name__ == '__main__':
    run()
