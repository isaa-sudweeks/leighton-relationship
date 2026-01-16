
import requests
import pandas as pd
import json
import time
import os
import re
from datetime import datetime
import warnings

# Suppress SSL warnings
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore")

from dotenv import load_dotenv
load_dotenv()

# Ensure data directory exists
os.makedirs("data", exist_ok=True)

# API Configuration
API_EMAIL = os.getenv("API_EMAIL")
API_KEY = os.getenv("API_KEY")

if not API_EMAIL or not API_KEY:
    print("Warning: API credentials not found in .env. Using defaults if available or exiting.")
    # Fallback to defaults validation or exit
    if not API_EMAIL: API_EMAIL = 'callumf@byu.edu'
    if not API_KEY: API_KEY = 'dunfrog78'

BASE_URL = "https://aqs.epa.gov/data/api"
STATE_CODE = "49"  # Utah

# Parameter Codes
PARAMS = {
    "44201": "O3",
    "42601": "NO",
    "42602": "NO2",
    "63301": "SR",
    "62101": "Temp"
}
PARAM_CODES_STR = ",".join(PARAMS.keys())

TIMEOUT = 20 # Increased to help with read timeouts

def get_session():
    """
    Creates a requests Session with retry logic.
    """
    session = requests.Session()
    retry = Retry(
        total=5, 
        backoff_factor=2, 
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        read=5, # specific retry for read errors
        connect=5 # specific retry for connection errors
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def wait_for_api():
    """Politeness delay"""
    time.sleep(1)

def get_sites(email, key, state_code):
    """Fetch all sites in the state."""
    print("Fetching counties...")
    session = get_session()
    try:
        counties_url = f"{BASE_URL}/list/countiesByState?email={email}&key={key}&state={state_code}"
        # print(f"DEBUG: GET {counties_url}")
        resp = session.get(counties_url, timeout=TIMEOUT, verify=False)
        
        if resp.status_code != 200:
            print("Error fetching counties:", resp.text)
            return []
        
        counties = resp.json().get('Data', [])
        all_sites = []
        
        print(f"Found {len(counties)} counties. Fetching sites...")
        for county in counties:
            county_code = county['code']
            
            sites_url = f"{BASE_URL}/list/sitesByCounty?email={email}&key={key}&state={state_code}&county={county_code}"
            s_resp = session.get(sites_url, timeout=TIMEOUT, verify=False)
            
            if s_resp.status_code == 200:
                sites_data = s_resp.json().get('Data', [])
                for site in sites_data:
                    site['county_code'] = county_code
                    site['county_name'] = county['value_represented']
                    all_sites.append(site)
            else:
                print(f"Warning: Could not fetch sites for county {county_code}")
        
        # Deduplicate
        unique_sites = {}
        for s in all_sites:
            uid = f"{state_code}-{s['county_code']}-{s['code']}"
            unique_sites[uid] = s
            
        return list(unique_sites.values())
    except Exception as e:
        print(f"Exception in get_sites: {e}")
        return []

def check_site_monitors(email, key, state, county, site):
    """
    Check if a site has EVER monitored the required parameters.
    """
    # Use a wide range to catch any historical monitors
    bdate = "19800101"
    edate = datetime.now().strftime("%Y%m%d")
    
    url = f"{BASE_URL}/monitors/bySite?email={email}&key={key}&state={state}&county={county}&site={site}&bdate={bdate}&edate={edate}"
    
    try:
        session = get_session()
        resp = session.get(url, timeout=TIMEOUT, verify=False)
        wait_for_api()
        
        if resp.status_code != 200:
            return False, []
        
        monitors = resp.json().get('Data', [])
        found_params = set()
        for m in monitors:
            if m['parameter_code'] in PARAMS:
                found_params.add(m['parameter_code'])
                
        required = set(PARAMS.keys())
        has_all = required.issubset(found_params) #I am not sure if this is right. 
        return has_all, list(found_params)
        
    except Exception as e:
        print(f"Exception checking monitors for {site}: {e}")
        return False, []

def fetch_hourly_data(email, key, state, county, site, bdate, edate):
    """Fetch hourly data for the specified date range and site."""
    url = f"{BASE_URL}/sampleData/bySite?email={email}&key={key}&param={PARAM_CODES_STR}&bdate={bdate}&edate={edate}&state={state}&county={county}&site={site}"
    try:
        session = get_session()
        resp = session.get(url, timeout=TIMEOUT, verify=False)
        wait_for_api() 
        
        if resp.status_code != 200:
            print(f"    Error fetching {bdate}-{edate}: {resp.status_code}")
            return []
        
        return resp.json().get('Data', [])
    except Exception as e:
        print(f"    Exception fetching data: {e}")
        return []

def sanitize_filename(name):
    return re.sub(r'[^\w\-_]', '_', name)

def main():
    START_YEAR = 1980
    END_YEAR = datetime.now().year
    
    # 1. Get Sites
    sites = get_sites(API_EMAIL, API_KEY, STATE_CODE)
    print(f"Total Sites in Utah: {len(sites)}")
    
    if not sites:
        print("No sites found. Check API/Network.")
        return

    for site_info in sites:
        site_id = site_info['code']
        county_code = site_info['county_code']
        site_name = site_info.get('value_represented')
        if not site_name or not site_name.strip():
             site_name = site_info.get('local_site_name', f"Site_{site_id}")
        
        safe_name = sanitize_filename(f"{site_name}_{site_id}")
        
        print(f"Processing Site: {site_name} (ID: {site_id}) in County {county_code}...")
        
        # 2. Monitor Check
        has_all, found = check_site_monitors(API_EMAIL, API_KEY, STATE_CODE, county_code, site_id)
        if not has_all:
             # msg = [PARAMS.get(p, p) for p in found]
             # print(f"  Skipping. Missing params.")
             continue
        
        print(f"  Site has all parameters. Fetching data...")
        
        all_records = []
        for year in range(START_YEAR, END_YEAR + 1):
            if year == datetime.now().year:
                 today_str = datetime.now().strftime("%Y%m%d")
                 bdate = f"{year}0101"
                 if bdate > today_str: break
                 edate = min(f"{year}1231", today_str)
            else:
                 bdate = f"{year}0101"
                 edate = f"{year}1231"
            
            # Optimization: Skip fetching if we already know no data? 
            # No, fetch_data is the source of truth.
            
            print(f"    Fetching {year}...")
            data = fetch_hourly_data(API_EMAIL, API_KEY, STATE_CODE, county_code, site_id, bdate, edate)
            all_records.extend(data)
            
        if not all_records:
            print("  No data records found.")
            continue
            
        # 3. Process
        print(f"  Processing {len(all_records)} records...")
        df = pd.DataFrame(all_records)
        if df.empty: continue

        df['datetime'] = pd.to_datetime(df['date_local'] + ' ' + df['time_local'])
        df['param_name'] = df['parameter_code'].map(PARAMS)
        
        # Metadata logic
        parameter_details = {}
        for p_code, p_name in PARAMS.items():
             subset = df[df['parameter_code'] == p_code]
             if not subset.empty:
                 details = {}
                 exclude = ['date_local', 'time_local', 'date_gmt', 'time_gmt', 'sample_measurement', 'datetime', 'param_name', 'parameter_code', 'state_code', 'county_code', 'site_number']
                 candidates = [c for c in subset.columns if c not in exclude]
                 for col in candidates:
                     unique_vals = subset[col].dropna().unique()
                     val_list = []
                     for v in unique_vals:
                         if hasattr(v, 'item'): v = v.item() 
                         val_list.append(v)
                     
                     if len(val_list) == 1:
                         details[col] = val_list[0]
                     elif len(val_list) > 1:
                         details[col] = val_list
                     else:
                         details[col] = None
                 parameter_details[p_name] = details
             else:
                 parameter_details[p_name] = "No Data"

        # Pivot
        df_pivoted = df.pivot_table(index='datetime', columns='param_name', values='sample_measurement', aggfunc='mean')
        
        # Save Raw
        raw_file = f"data/{safe_name}_raw.csv"
        df_pivoted.to_csv(raw_file)
        print(f"  Saved RAW: {raw_file}")
        
        # Clean
        required_cols = ['O3', 'NO', 'NO2', 'SR', 'Temp']
        df_pivoted = df_pivoted.reindex(columns=required_cols)
        df_cleaned = df_pivoted.dropna()
        
        cleaned_file = f"data/{safe_name}_cleaned.csv"
        df_cleaned.to_csv(cleaned_file)
        print(f"  Saved CLEANED: {cleaned_file} ({len(df_cleaned)} rows)")
        
        # Save Metadata
        metadata = {
            "site_name": site_name,
            "site_id": site_id,
            "county_code": county_code,
            "params": required_cols,
            "details": parameter_details,
            "raw_file": raw_file,
            "cleaned_file": cleaned_file
        }
        meta_file = f"data/{safe_name}_metadata.json"
        with open(meta_file, 'w') as f:
            json.dump(metadata, f, indent=4)
        print(f"  Saved Metadata: {meta_file}")

if __name__ == "__main__":
    main()
