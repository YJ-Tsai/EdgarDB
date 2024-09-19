import requests
from datetime import datetime, timedelta
import os
import time

headers = {'User-Agent': 'Yung-Jen Tsai (YJ-T@github.com)'}

def get_quarter(month):
    return (month - 1) // 3 + 1

def download_new_index_files(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        quarter = f"QTR{get_quarter(current_date.month)}"
        date_str = current_date.strftime('%Y%m%d')
        idx_url = f'https://www.sec.gov/Archives/edgar/daily-index/{year}/{quarter}/company.{date_str}.idx'
        
        response = requests.get(idx_url, headers=headers)
        if response.status_code == 200:
            idx_dir = os.path.join('index_files', str(year))
            os.makedirs(idx_dir, exist_ok=True)
            idx_file_path = os.path.join(idx_dir, f'company_{date_str}.idx')
            with open(idx_file_path, 'wb') as f:
                f.write(response.content)
            print(f'Downloaded index file: {idx_file_path}')
        else:
            print(f'Index file not found for date: {date_str}')
        
        time.sleep(0.1)  # Be polite and avoid overloading SEC servers
        current_date += timedelta(days=1)

# Example usage:
# Start date: the day after your last data entry
# End date: today's date
start_date = datetime(2024, 9, 18)
end_date = datetime.now().date()

download_new_index_files(start_date, end_date)
