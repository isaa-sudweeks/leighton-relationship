import pandas as pd
import requests
import json
import os
import sys
from dotenv import load_dotenv
from datetime import datetime
import argparse

# Load environment variables
load_dotenv()
MESOWEST_TOKEN = os.getenv('MESOWEST_TOKEN')

def load_data(csv_path, json_path):
    """
    Loads the CSV data and JSON metadata.
    """
    try:
        df = pd.read_csv(csv_path)
        # Ensure 'datetime' column is datetime objects
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        with open(json_path, 'r') as f:
            metadata = json.load(f)
            
        return df, metadata
    except Exception as e:
        print(f"Error loading files: {e}")
        sys.exit(1)

def find_nearest_station(lat, lon, token, limit_radius_miles=25):
    """
    Finds the nearest Synoptic station within radius that has solar_radiation.
    """
    url = "https://api.synopticdata.com/v2/stations/metadata"
    params = {
        "token": token,
        "lat": lat,
        "lon": lon,
        "radius": limit_radius_miles, 
        "vars": "solar_radiation",
        "limit": 1
    }
    
    # Correction: 'radius' param in Synoptic API with lat/lon is comma separated string: "lat,lon,dist_miles"
    # OR we can use separate lat, lon, radius params if the library supports it, but standard REST API is often "radius=lat,lon,miles"
    # Let's use the standard "radius" parameter formatted string to be safe.
    
    formatted_radius = f"{lat},{lon},{limit_radius_miles}"
    
    # We don't use separate lat/lon params if we use the radius string format usually, 
    # but the API allows `&lat=...&lon=...&radius=...` too. 
    # Let's clean up params for the request
    req_params = {
        "token": token,
        "radius": formatted_radius,
        "vars": "solar_radiation",
        "limit": 1
    }

    try:
        response = requests.get(url, params=req_params)
        response.raise_for_status()
        data = response.json()
        
        if not data.get('STATION'):
            print(f"No stations found with solar_radiation within {limit_radius_miles} miles.")
            return None, None

        station = data['STATION'][0]
        return station['STID'], station
        
    except Exception as e:
        print(f"Error finding nearest station: {e}")
        return None, None

def fetch_synoptic_data(station_id, start_time, end_time, token, station_timezone):
    """
    Fetches timeseries data for the station.
    """
    url = "https://api.synopticdata.com/v2/stations/timeseries"
    
    # Format dates as YYYYMMDDHHMM
    start_str = start_time.strftime('%Y%m%d%H%M')
    end_str = end_time.strftime('%Y%m%d%H%M')
    
    params = {
        "token": token,
        "stid": station_id,
        "start": start_str,
        "end": end_str,
        "vars": "solar_radiation",
        "obtimezone": "local" 
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if not data.get('STATION'):
            print("No data found for this station in the given time range.")
            return pd.DataFrame()
            
        observations = data['STATION'][0].get('OBSERVATIONS', {})
        
        if 'date_time' not in observations or 'solar_radiation_set_1' not in observations:
            print("Expected data fields missing in response.")
            return pd.DataFrame()

        # Parse with utc=True to handle mixed offsets (DST)
        dates = pd.to_datetime(observations['date_time'], utc=True)
        
        # Convert to station's local time if timezone is available
        if station_timezone:
            try:
                dates = dates.tz_convert(station_timezone)
            except Exception as e:
                print(f"Warning: Could not convert to timezone {station_timezone}: {e}")
        
        # Make naive to match CSV (stripping offset, keeping wall time)
        dates = dates.tz_localize(None)


        synoptic_df = pd.DataFrame({
            'datetime_synoptic': dates,
            'SR_Synoptic': observations['solar_radiation_set_1']
        })
        
        # Get Units and Sensor Variables if available
        units = data.get('UNITS', {})
        
        # Need to find the station object again for sensor vars
        station_data = data['STATION'][0]
        sensor_vars = station_data.get('SENSOR_VARIABLES', {})
        
        return synoptic_df, units, sensor_vars
        
    except Exception as e:
        print(f"Error fetching timeseries data: {e}")
        return pd.DataFrame(), {}, {}

def fuse_data(original_df, synoptic_df):
    """
    Merges synoptic data into original dataframe.
    """
    if synoptic_df.empty:
        original_df['SR_Synoptic'] = None
        return original_df
        
    # Data is already standardized to naive datetime in fetch_synoptic_data

    # Rename for merging
    synoptic_df = synoptic_df.rename(columns={'datetime_synoptic': 'datetime'})
    
    # Drop existing synoptic columns to avoid duplicates (x, y suffix)
    cols_to_drop = [c for c in original_df.columns if 'SR_Synoptic' in c]
    if cols_to_drop:
        print(f"Dropping existing columns: {cols_to_drop}")
        original_df = original_df.drop(columns=cols_to_drop)
    
    # Merge left to keep all original rows
    # Using merge_asof is safer for slight time diffs, but let's stick to simple merge if we assume hourly alignment
    # Or just generic merge.
    
    merged_df = pd.merge(original_df, synoptic_df, on='datetime', how='left')
    
    return merged_df

def main():
    # Setup Argument Parser
    parser = argparse.ArgumentParser(description='Fuse Synoptic SR data into existing CSV.')
    parser.add_argument('csv_path', help='Path to the raw CSV file')
    parser.add_argument('json_path', help='Path to the metadata JSON file')
    
    args = parser.parse_args()
    
    if not MESOWEST_TOKEN:
        print("Error: MESOWEST_TOKEN not found in .env file.")
        sys.exit(1)
        
    print(f"Processing {args.csv_path}...")
    
    # 1. Load Data
    df, metadata = load_data(args.csv_path, args.json_path)
    
    # Get Location from Metadata
    # The metadata structure has params nested. Let's look for a general lat/lon 
    # OR pick one from the existing params (like 'O3' or 'SR' which has lat/lon).
    # Based on the user provided file, 'SR' or 'O3' object has lat/lon.
    # Let's use the first available one.
    
    lat = None
    lon = None
    
    if 'details' in metadata:
        for param in metadata['details']:
            if 'latitude' in metadata['details'][param] and 'longitude' in metadata['details'][param]:
                lat = metadata['details'][param]['latitude']
                lon = metadata['details'][param]['longitude']
                break
    
    if lat is None or lon is None:
        print("Could not find latitude/longitude in metadata.")
        sys.exit(1)
        
    print(f"Location: {lat}, {lon}")
    
    # 2. Find Nearest Station
    station_id, station_meta = find_nearest_station(lat, lon, MESOWEST_TOKEN)
    
    if not station_id:
        print("Aborting: No suitable Synoptic station found.")
        # Create empty column just in case? Or exit? User probably prefers exit or warning.
        # But let's create the column with NaNs to fulfill "add a new column" requirement
        # Check if column already exists to avoid overwriting with all None if rerunning
        if 'SR_Synoptic' not in df.columns:
            df['SR_Synoptic'] = None
        df.to_csv(args.csv_path, index=False)
        sys.exit(0)
    
    print(f"Found nearest station: {station_id} ({station_meta.get('NAME', 'Unknown')}) at distance {station_meta.get('DISTANCE', '?')} miles")
    
    # 3. Fetch Data
    start_time = df['datetime'].min()
    end_time = df['datetime'].max()
    print(f"Fetching data from {start_time} to {end_time}...")
    
    station_timezone = station_meta.get('TIMEZONE')
    synoptic_data, units, sensor_vars = fetch_synoptic_data(station_id, start_time, end_time, MESOWEST_TOKEN, station_timezone)
    
    # 4. Fuse Data
    # Only keep the 'SR_Synoptic' column
    print("Fusing data...")
    final_df = fuse_data(df, synoptic_data)
    
    # 5. Save CSV
    final_df.to_csv(args.csv_path, index=False)
    print(f"Updated CSV saved to {args.csv_path}")
    
    # 6. Update Metadata
    if 'synoptic_source' not in metadata:
        metadata['synoptic_source'] = {}
    
    metadata['synoptic_source'] = {
        "station_id": station_id,
        "station_name": station_meta.get('NAME'),
        "distance_miles": station_meta.get('DISTANCE'),
        "vars_fetched": ["solar_radiation"],
        "fetch_date": datetime.now().isoformat(),
        "units": units,
        "sensor_variables": sensor_vars
    }
    
    with open(args.json_path, 'w') as f:
        json.dump(metadata, f, indent=4)
    print(f"Updated metadata saved to {args.json_path}")

if __name__ == "__main__":
    main()
