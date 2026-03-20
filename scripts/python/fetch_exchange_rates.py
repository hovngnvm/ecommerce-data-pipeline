import requests
import json
import os, sys

def fetch_historical_rates():

    # Frankfurter API does not support VND, so we use EUR, JPY to demo multi-currency Join.
    url = "https://api.frankfurter.app/2019-10-01..2019-10-31?from=USD&to=EUR,JPY"
    response = requests.get(url)
    
    OUTPUT_FILE = sys.argv[1]

    if response.status_code == 200: # 200 is the standard HTTP status for "OK"
        data = response.json()
        
        output_dir = os.path.dirname(OUTPUT_FILE)
        os.makedirs(output_dir, exist_ok=True)  # Create directory if it doesn't exist
        
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
    else:
        response.raise_for_status()

if __name__ == "__main__":
    fetch_historical_rates()
    