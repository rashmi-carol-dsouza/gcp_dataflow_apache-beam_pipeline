import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from geopy.distance import geodesic
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def calculate_distance(row, stations_dict):
    start_station = stations_dict.get(row['start_station_id'])
    end_station = stations_dict.get(row['end_station_id'])
    if start_station and end_station:
        start_coords = (start_station['latitude'], start_station['longitude'])
        end_coords = (end_station['latitude'], end_station['longitude'])
        distance = geodesic(start_coords, end_coords).kilometers
        row['distance'] = distance
    else:
        row['distance'] = None
    return row

def run():
    # Define the pipeline options
    options = PipelineOptions()
    gcloud_options = options.view_as(GoogleCloudOptions)
    gcloud_options.project = 'ml-6-coding-challenge-425112'
    gcloud_options.job_name = 'london-bike-dataflow'
    gcloud_options.staging_location = 'gs://ml6-bucket-11/staging'
    gcloud_options.temp_location = 'gs://ml6-bucket-11/temp'
    gcloud_options.region = 'eu-west1'

    with beam.Pipeline(options=options) as p:
        # Read station data from BigQuery
        logging.info("Reading station data from BigQuery.")
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
        logging.info("Reading ride data from BigQuery.")
        rides = (
            p | 'Read Rides Data' >> beam.io.ReadFromBigQuery(
                query='''
                    SELECT start_station_id, end_station_id
                    FROM `bigquery-public-data.london_bicycles.cycle_hire`
                ''',
                use_standard_sql=True
            )
        )

        # Convert stations PCollection to dictionary
        logging.info("Converting stations data to dictionary.")
        stations_dict = beam.pvalue.AsDict(
            stations | 'Map Stations to Dict' >> beam.Map(lambda x: (x['station_id'], x))
        )

        # Calculate the number of rides for each combination of start and end stations
        logging.info("Counting the number of rides for each station pair.")
        ride_counts = (
            rides
            | 'Create Key-Value Pairs' >> beam.Map(lambda x: ((x['start_station_id'], x['end_station_id']), 1))
            | 'Count Rides' >> beam.CombinePerKey(sum)
            | 'Format Ride Count Output' >> beam.Map(lambda x: {'start_station_id': x[0][0], 'end_station_id': x[0][1], 'ride_count': x[1]})
        )

        # Calculate the distance for each ride and format the output
        logging.info("Calculating distances for each ride.")
        ride_distances = (
            ride_counts
            | 'Calculate Distances' >> beam.Map(calculate_distance, stations_dict=stations_dict)
            | 'Filter Valid Distances' >> beam.Filter(lambda x: x['distance'] is not None)
            | 'Format Distance Output' >> beam.Map(lambda x: f"{x['start_station_id']},{x['end_station_id']},{x['ride_count']},{x['distance']:.2f}")
        )

        # Write the results to Google Cloud Storage
        logging.info("Writing the results to Google Cloud Storage.")
        ride_distances | 'Write to GCS' >> beam.io.WriteToText('gs://ml6-bucket-11/output/ride_distances', file_name_suffix='.txt')

if __name__ == '__main__':
    run()
