import asyncio
import zendriver as zd
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

jobs_search_keyword = [
    "software+engineer",
]
locations = ["gujranwala"]
data = {
    'title': [],
    'location': [],
    'company': [],
    'salary': [],
    'created_at': [],
    'url': [],
}

def get_url(query, loc, start):
    return f"https://pk.indeed.com/jobs?q={query}&l={loc}&radius=35&start={start}"

def write_to_csv(data, file_path):
    try:
        new_df = pd.DataFrame(data)
        new_df['title'] = new_df['title'].astype(str).str.strip()
        new_df['company'] = new_df['company'].astype(str).str.strip()
        new_df['location'] = new_df['location'].astype(str).str.strip()
        
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path)
            existing_df['title'] = existing_df['title'].astype(str).str.strip()
            existing_df['company'] = existing_df['company'].astype(str).str.strip()
            existing_df['location'] = existing_df['location'].astype(str).str.strip()
            
            # Create multi-indexes for new and existing data based on title, company, and location
            new_index = new_df.set_index(['title', 'company', 'location']).index
            existing_index = existing_df.set_index(['title', 'company', 'location']).index
            
            # Filter rows in new_df that are not already in existing_df based on the multi-index
            new_data = new_df[~new_index.isin(existing_index)]
            
            # Combine the data and drop any duplicates based on title, company, and location
            combined_df = pd.concat([existing_df, new_data]).drop_duplicates(subset=['title', 'company', 'location'], keep='first')
        else:
            new_data = new_df
            combined_df = new_df
            
        combined_df.to_csv(file_path, index=False)
        return new_data
    except Exception as e:
        print(f"An error occurred while writing to CSV: {e}")
        return None

async def scrape_jobs():
    browser = await zd.start()
    for query in jobs_search_keyword:
        for loc in locations:
            start = 0
            page = 0  # Track pagination start parameter
            url = get_url(query, loc, start)
            current_page = await browser.get(url)
            time.sleep(20)

            while True:
                await current_page.wait_for_ready_state(until="complete")
                # Find job cards
                contents = await current_page.find_elements_by_text("slider_item")
                
                if not contents:
                    print(f"No jobs found for {query} in {loc} on page {page}.")
                    break
                
                # Process job listings
                for content in contents:
                    soup = BeautifulSoup(str(content), 'html.parser')
                    
                    # Extract data with error handling, setting default values if extraction fails
                    try:
                        title = soup.find('h2').text.strip()
                    except:
                        title = "N/A"

                    try:
                        job_location = soup.find(class_="css-1restlb").text.strip()
                    except:
                        job_location = "N/A"

                    try:
                        company = soup.find('span', {'data-testid': 'company-name'}).text.strip()
                    except:
                        company = "N/A"
                    
                    try:
                        half_url = soup.find(class_="jcs-JobTitle")
                        href = f"http://www.indeed.com{half_url.get('href')}"
                    except:
                        href = "N/A"
                    
                    # Append to data
                    data['title'].append(title)
                    data['location'].append(job_location)
                    data['company'].append(company)
                    data["salary"].append("$120,000 - $150,000 per year")
                    data["created_at"].append(time.strftime("%Y-%m-%d"))
                    data['url'].append(href)

                # Check for next page
                try:
                    next_buttons = await current_page.find_element_by_text("pagination-page-next")
                except Exception:
                    next_buttons = None

                if not next_buttons:
                    break
                else:
                    await next_buttons.click()
                    page += 10
                    time.sleep(20)
                    await current_page.wait_for_ready_state(until="complete")

    # Save data to a CSV file
    file_path = "jobs.csv"
    u_data = write_to_csv(data, file_path)
    await browser.stop()
    return u_data


