import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import logging

def run():
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Define the pipeline options
    options = PipelineOptions()
    gcloud_options = options.view_as(GoogleCloudOptions)
    gcloud_options.project = 'ml-6-coding-challenge-425112'
    gcloud_options.job_name = 'read-bigquery-data'
    gcloud_options.staging_location = 'gs://ml6-bucket-11/staging'
    gcloud_options.temp_location = 'gs://ml6-bucket-11/temp'
    gcloud_options.region = 'eu-west1'  # Ensure this matches your dataset region

    with beam.Pipeline(options=options) as p:
        # Read station data from BigQuery
        stations = (
            p | 'Read Stations Data' >> beam.io.ReadFromBigQuery(
                query='''
                    SELECT id AS station_id, latitude, longitude
                    FROM `bigquery-public-data.london_bicycles.cycle_stations`
                ''',
                use_standard_sql=True
            )
        )

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

        # Log some outputs to verify the data
        stations | 'Log Stations' >> beam.Map(logging.info)
        rides | 'Log Rides' >> beam.Map(logging.info)

if __name__ == '__main__':
    run()
