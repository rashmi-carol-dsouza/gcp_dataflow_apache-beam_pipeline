import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
import logging
from math import radians, cos, sin, sqrt, atan2

class FormatResult(beam.DoFn):
    def process(self, element):
        (start_station_id, end_station_id, ride_count, total_distance_km) = element
        yield f"{start_station_id},{end_station_id},{ride_count},{total_distance_km}"

class CalculateDistance(beam.DoFn):
    def process(self, element, stations_dict):
        start_station_id, end_station_id, ride_count = element
        if start_station_id in stations_dict and end_station_id in stations_dict:
            start_coords = stations_dict[start_station_id]
            end_coords = stations_dict[end_station_id]
            distance = self.haversine(start_coords['latitude'], start_coords['longitude'],
                                      end_coords['latitude'], end_coords['longitude'])
            total_distance = distance * ride_count
            yield (start_station_id, end_station_id, ride_count, total_distance)
    
    def haversine(self, lat1, lon1, lat2, lon2):
        R = 6371  # Radius of the earth in km
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat/2) * sin(dlat/2) + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2) * sin(dlon/2)
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        return R * c

def run():
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Define the pipeline options
    options = PipelineOptions()
    gcloud_options = options.view_as(GoogleCloudOptions)
    gcloud_options.project = 'ml-6-coding-challenge-425112'
    gcloud_options.job_name = 'calculate-distance'
    gcloud_options.staging_location = 'gs://ml6-challenge-dataflow-bucket/staging'
    gcloud_options.temp_location = 'gs://ml6-challenge-dataflow-bucket/temp'
    gcloud_options.region = 'europe-west1'
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        # Read station data from BigQuery
        stations = (
            p | 'Read Stations Data' >> beam.io.ReadFromBigQuery(
                query='''SELECT id AS station_id, latitude, longitude FROM `bigquery-public-data.london_bicycles.cycle_stations`''',
                use_standard_sql=True
            )
        )

        # Read ride data from BigQuery
        rides = (
            p | 'Read Rides Data' >> beam.io.ReadFromBigQuery(
                query='''SELECT start_station_id, end_station_id FROM `bigquery-public-data.london_bicycles.cycle_hire`''',
                use_standard_sql=True
            )
        )

        # Filter out None values and pair rides
        filtered_rides = (
            rides
            | 'Filter None Values' >> beam.Filter(lambda row: row['start_station_id'] is not None and row['end_station_id'] is not None)
            | 'Pair Rides' >> beam.Map(lambda row: (row['start_station_id'], row['end_station_id']))
        )

        # Count the number of rides for each start and end station combination
        ride_counts = (
            filtered_rides
            | 'Count Rides' >> beam.combiners.Count.PerElement()
        )

        # Prepare the stations dictionary for side input
        stations_dict = (
            stations
            | 'Map Stations to Dict' >> beam.Map(lambda row: (row['station_id'], {'latitude': row['latitude'], 'longitude': row['longitude']}))
            | 'Stations as Dict' >> beam.combiners.ToDict()
        )

        # Calculate distances
        distances = (
            ride_counts
            | 'Calculate Distance' >> beam.ParDo(CalculateDistance(), beam.pvalue.AsDict(stations_dict))
        )

        # Format and write the results to a text file
        formatted_results = (
            distances
            | 'Format Results' >> beam.ParDo(FormatResult())
            | 'Write to Text' >> beam.io.WriteToText('gs://ml6-challenge-dataflow-bucket/output/hard_test_rides_distance', shard_name_template='')
        )

if __name__ == '__main__':
    run()
