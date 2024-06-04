import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from geopy.distance import geodesic


def format_station_kv(row):
    return row['station_id'], {'latitude': row['latitude'], 'longitude': row['longitude']}

def enrich_ride_data(element, stations_dict):
    (start_station_id, end_station_id), count = element
    start_station = stations_dict.get(start_station_id, {'latitude': None, 'longitude': None})
    end_station = stations_dict.get(end_station_id, {'latitude': None, 'longitude': None})
    
    # Calculate distance if coordinates are available
    if all(value is not None for value in [start_station['latitude'], start_station['longitude'], end_station['latitude'], end_station['longitude']]):
        distance = calculate_distance((start_station['latitude'], start_station['longitude']), (end_station['latitude'], end_station['longitude']))
    else:
        distance = 0  # Setting distance to 0 if coordinates are not available
        
    total_kilometers = count * distance  # Calculate total kilometers
    
    return {
        'start_station_id': start_station_id,
        'end_station_id': end_station_id,
        'count': count,
        'distance': distance,
        'total_kilometers': total_kilometers
    }

def calculate_distance(start_coords, end_coords):
    return geodesic(start_coords, end_coords).kilometers

def format_output(element):
    return f"{element['start_station_id']},{element['end_station_id']},{element['count']},{element['total_kilometers']}"

def filter_null_ids(row):
    return row['start_station_id'] is not None and row['end_station_id'] is not None

def filter_null_coordinates(element):
    return element['latitude_start'] is not None and element['longitude_start'] is not None and element['latitude_end'] is not None and element['longitude_end'] is not None

def run():
    # Set up the pipeline options
    options = PipelineOptions()
    gcloud_options = options.view_as(GoogleCloudOptions)
    gcloud_options.project = 'ml-6-coding-challenge-425112'
    gcloud_options.job_name = 'calculate-ride-counts'
    gcloud_options.staging_location = 'gs://ml6-challenge-dataflow-bucket/staging'
    gcloud_options.temp_location = 'gs://ml6-challenge-dataflow-bucket/temp'
    gcloud_options.region = 'europe-west1'
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(SetupOptions).requirements_file = 'requirements.txt'

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

        # Filter out any records with NULL station IDs
        valid_rides = rides | 'Filter Null IDs' >> beam.Filter(filter_null_ids)

        # Format station data
        station_kv = stations | 'Format Station Data' >> beam.Map(format_station_kv)

        # Create dictionaries for stations
        stations_dict = beam.pvalue.AsDict(station_kv)

        # Count the number of rides for each start and end station combination
        ride_counts = (
            valid_rides
            | 'Pair Rides' >> beam.Map(lambda row: ((row['start_station_id'], row['end_station_id']), 1))
            | 'Count Rides' >> beam.combiners.Count.PerKey()
        )

        # Enrich ride data with station coordinates
        enriched_rides = ride_counts | 'Enrich Rides' >> beam.Map(enrich_ride_data, stations_dict)

        # Filter out entries with None coordinates
        valid_enriched_rides = enriched_rides | 'Filter Null Coordinates' >> beam.Filter(lambda x: x['distance'] > 0)

        # Format the output
        formatted_ride_counts = valid_enriched_rides | 'Format Ride Counts' >> beam.Map(format_output)

        # Log ride counts
        formatted_ride_counts | 'Print Ride Counts' >> beam.Map(print)

        # Write the results to a text file in GCS
        formatted_ride_counts | 'Write to GCS' >> beam.io.WriteToText('gs://ml6-challenge-dataflow-bucket/output/enriched_ride_counts', shard_name_template='', append_trailing_newlines=True)

if __name__ == '__main__':
    run()
