import requests
import os
import time

headers = {'User-Agent': 'Tony Tsai (yungt@hotmail.com)'}

def download_index_files(start_year, end_year):
    for year in range(start_year, end_year + 1):
        for quarter in range(1, 5):
            idx_url = f'https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/company.idx'
            response = requests.get(idx_url, headers=headers)
            if response.status_code == 200:
                idx_dir = os.path.join('index_files', str(year))
                os.makedirs(idx_dir, exist_ok=True)
                idx_file_path = os.path.join(idx_dir, f'company_{year}_QTR{quarter}.idx')
                with open(idx_file_path, 'wb') as f:
                    f.write(response.content)
                print(f'Downloaded index file: {idx_file_path}')
            else:
                print(f'Failed to download index file for {year} QTR{quarter}')
            time.sleep(0.5)  # Be polite and avoid overloading SEC servers

download_index_files(2018, 2024)
