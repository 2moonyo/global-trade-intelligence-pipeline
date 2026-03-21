import requests
from bs4 import BeautifulSoup
import csv
import json
import time
from datetime import datetime
import os

# --- CONFIGURATION ---
os.makedirs("data/bronze/ener/weekly_links", exist_ok=True)
os.makedirs("data/metadata/ener/weekly_links", exist_ok=True)
START_DATE = "01/01/2024"  # DD/MM/YYYY
END_DATE = "11/03/2026"    # DD/MM/YYYY
BASE_URL = "https://ec.europa.eu/newsroom/ener/newsletter-archives/view/service/238"
CSV_FILENAME = f"data/bronze/ener/weekly_links/oil_bulletin_links_start{START_DATE.replace('/', '-')}_end{END_DATE.replace('/', '-')}_runtime{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
MANIFEST_FILENAME = f"data/metadata/ener/weekly_links/scrape_manifest{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.jsonl"

def fetch_with_retry(url, retries=3, delay=5):
    for i in range(retries):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            return response
        except Exception as e:
            print(f"Attempt {i+1} failed for {url}: {e}")
            if i < retries - 1:
                time.sleep(delay)
    return None

def scrape_bulletin_links(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%d/%m/%Y")
    end_date = datetime.strptime(end_date_str, "%d/%m/%Y")
    
    all_extracted_data = []
    manifest_entries = []
    page = 1
    in_range = True

    while in_range:
        url = f"{BASE_URL}?page={page}"
        print(f"Scraping Page {page}...")
        
        response = fetch_with_retry(url)
        if not response:
            print("No more pages found or connection failed.")
            break

        soup = BeautifulSoup(response.content, 'html.parser')
        rows = soup.select('#main-content table tbody tr')

        if not rows:
            print("No data found on page. Ending search.")
            break

        page_has_valid_data = False
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 3: continue

            # Extract Data
            date_text = cells[1].get_text(strip=True)
            link_tag = cells[2].find('a')
            
            if not date_text or not link_tag: continue
            
            pub_date = datetime.strptime(date_text, "%d/%m/%Y")
            title = link_tag.get_text(strip=True)
            link = "https://ec.europa.eu" + link_tag['href'] if link_tag['href'].startswith('/') else link_tag['href']

            # Date Range Validation
            if start_date <= pub_date <= end_date:
                page_has_valid_data = True
                entry = {
                    "publish_date": date_text,
                    "title": title,
                    "url": link,
                    "load_ts": datetime.now().isoformat()
                }
                all_extracted_data.append(entry)
                
                # Manifest entry
                manifest_entries.append({
                    "run_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "source_url": url,
                    "origin_page": page,
                    "extracted_at": datetime.now().isoformat(),
                    "link_title": title,
                    "target_link": link
                })
            
            elif pub_date < start_date:
                in_range = False # Archive is usually chronological, stop if we pass the end date
                break

        if not page_has_valid_data and in_range:
             print(f"No articles in range on page {page}.")

        page += 1
        time.sleep(1) # Polite scraping

    # Save to CSV
    if all_extracted_data:
        keys = all_extracted_data[0].keys()
        with open(CSV_FILENAME, 'w', newline='', encoding='utf-8') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(all_extracted_data)

        # Save to JSONL Manifest
        with open(MANIFEST_FILENAME, 'a', encoding='utf-8') as f:
            for entry in manifest_entries:
                f.write(json.dumps(entry) + '\n')
        
        print(f"Successfully extracted {len(all_extracted_data)} links.")
    else:
        print("Error: No data found for the specified range.")

if __name__ == "__main__":
    scrape_bulletin_links("01/01/2024", "11/03/2026")