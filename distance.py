import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import logging
from geopy.distance import geodesic

class CalculateDistance(beam.DoFn):
    def process(self, element, stations_dict):
        start_station_id, end_station_id, ride_count = element['start_station_id'], element['end_station_id'], element['ride_count']

        start_coords = stations_dict.get(start_station_id)
        end_coords = stations_dict.get(end_station_id)

        if start_coords and end_coords:
            distance_km = geodesic(start_coords, end_coords).kilometers
            total_distance_km = distance_km * ride_count
            yield (start_station_id, end_station_id, ride_count, total_distance_km)

def run():
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Define the pipeline options
    options = PipelineOptions()
    gcloud_options = options.view_as(GoogleCloudOptions)
    gcloud_options.project = 'ml-6-coding-challenge-425112'
    gcloud_options.job_name = 'calculate-distance'
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

        # Convert station data to a dictionary for side input
        stations_dict = (
            stations
            | 'Convert to KV' >> beam.Map(lambda row: (row['station_id'], (row['latitude'], row['longitude'])))
        )

        # Read ride data from BigQuery
        rides = (
            p | 'Read Rides Data' >> beam.io.ReadFromBigQuery(
                query='''
                    SELECT start_station_id, end_station_id, COUNT(*) AS ride_count
                    FROM `bigquery-public-data.london_bicycles.cycle_hire`
                    WHERE start_station_id IS NOT NULL AND end_station_id IS NOT NULL
                    GROUP BY start_station_id, end_station_id
                ''',
                use_standard_sql=True
            )
        )

        # Calculate distances and format results
        ride_distances = (
            rides
            | 'Calculate Distance' >> beam.ParDo(CalculateDistance(), beam.pvalue.AsDict(stations_dict))
        )

        # Write the results to a text file
        ride_distances | 'Write to Text' >> beam.io.WriteToText('gs://ml6-bucket-11/output/ride_distances', shard_name_template='')

if __name__ == '__main__':
    run()
