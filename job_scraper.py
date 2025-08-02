from seleniumbase import Driver
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

jobs_search_keyword = ["software+engineer"]
locations = ["gujranwala"]

def get_url(query, loc, start):
    return f"https://pk.indeed.com/jobs?q={query}&l={loc}&radius=35&start={start}"

def write_to_csv(data, file_path):
    try:
        print("Writing data to CSV...")
        new_df = pd.DataFrame(data)
        new_df['title'] = new_df['title'].astype(str).str.strip()
        new_df['company'] = new_df['company'].astype(str).str.strip()
        new_df['location'] = new_df['location'].astype(str).str.strip()

        if os.path.exists(file_path):
            print("Existing CSV found. Checking for duplicates...")
            try:
                existing_df = pd.read_csv(file_path)
                existing_df['title'] = existing_df['title'].astype(str).str.strip()
                existing_df['company'] = existing_df['company'].astype(str).str.strip()
                existing_df['location'] = existing_df['location'].astype(str).str.strip()

                new_index = new_df.set_index(['title', 'company', 'location']).index
                existing_index = existing_df.set_index(['title', 'company', 'location']).index

                new_data = new_df[~new_index.isin(existing_index)]
                combined_df = pd.concat([existing_df, new_data]).drop_duplicates(subset=['title', 'company', 'location'], keep='first')
            except pd.errors.EmptyDataError:
                print("Warning: jobs.csv is empty or invalid. Overwriting with new data.")
                new_data = new_df
                combined_df = new_df
        else:
            print("No existing CSV. Creating new one...")
            new_data = new_df
            combined_df = new_df

        combined_df.to_csv(file_path, index=False)
        print(f"Saved data to {file_path}.")
        return new_data
    except Exception as e:
        print(f"An error occurred while writing to CSV: {e}")
        return None

def scrape_jobs():
    print("Launching browser with SeleniumBase...")

    # Initialize clean data dictionary
    data = {
        'title': [],
        'location': [],
        'company': [],
        'salary': [],
        'created_at': [],
        'url': [],
    }

    with Driver(uc=True, headless=False) as driver:
        for query in jobs_search_keyword:
            for loc in locations:
                start = 0
                page = 0
                while True:
                    url = get_url(query, loc, start)
                    print(f"\n[Page {page}] Visiting URL: {url}")
                    driver.get(url)
                    time.sleep(5)

                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    job_cards = soup.find_all('div', class_='slider_item')

                    if not job_cards:
                        print(f"No jobs found for '{query}' in '{loc}' on page {page}.")
                        break

                    print(f"Found {len(job_cards)} job(s) on page {page}. Parsing...")

                    for idx, card in enumerate(job_cards):
                        print(f"  -> Extracting job {idx + 1} of {len(job_cards)}")
                        try:
                            title = card.find('h2').text.strip()
                        except:
                            title = "N/A"

                        try:
                            job_location = card.find(class_="css-1restlb").text.strip()
                        except:
                            job_location = "N/A"

                        try:
                            company = card.find('span', {'data-testid': 'company-name'}).text.strip()
                        except:
                            company = "N/A"

                        try:
                            job_url = card.find('a', class_="jcs-JobTitle")["href"]
                            href = f"https://pk.indeed.com{job_url}"
                        except:
                            href = "N/A"

                        try:
                            salary_tag = card.find('div', {'data-testid': 'attribute_snippet_testid'})
                            salary = salary_tag.text.strip() if salary_tag else "N/A"
                        except:
                            salary = "N/A"

                        data['title'].append(title)
                        data['location'].append(job_location)
                        data['company'].append(company)
                        data["salary"].append(salary)
                        data["created_at"].append(time.strftime("%Y-%m-%d"))
                        data['url'].append(href)

                        time.sleep(1)  # slight delay between job cards

                    # Try to click on "Next" page
                    try:
                        next_button = driver.find_element('css selector', 'a[aria-label="Next"]')
                        print("Next page found. Moving to next page...")
                        start += 10
                        page += 1
                        next_button.click()
                        time.sleep(4)
                    except Exception as e:
                        print("No more pages found or error during pagination.")
                        break

    file_path = "jobs.csv"
    return write_to_csv(data, file_path)
