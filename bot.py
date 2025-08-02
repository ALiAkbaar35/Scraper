import requests
import os
from job_scraper import scrape_jobs
import schedule
import time
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("URL")

def Post_job():
    try:
        # Directly call the synchronous scraper
        jobs_dataframe = scrape_jobs()
        
        if jobs_dataframe is not None and not jobs_dataframe.empty:
            json_records = jobs_dataframe.to_dict(orient='records')
            print("Posting data:", json_records)
            
            try:
                response = requests.post(url, json=json_records)
                print(f"Response status: {response.status_code}")
            except Exception as e:
                print(f"Error sending data: {e}")    
        else:
            print("No new job data to post.")
            
    except Exception as e:
        print("An error occurred:", e)

if __name__ == "__main__":
    # Run every minute
    schedule.every(1).minutes.do(Post_job)
    
    print("Scheduler started. Running every 1 minute.")
    while True:
        schedule.run_pending()
        time.sleep(15)
