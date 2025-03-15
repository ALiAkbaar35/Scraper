import asyncio
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
        # Get the DataFrame from the async scraper
        jobs_dataframe = asyncio.run(scrape_jobs())
        
        if jobs_dataframe is not None and not jobs_dataframe.empty:
            # Convert DataFrame to JSON-serializable format
            json_records = jobs_dataframe.to_dict(orient='records')
            print("Posting data:", json_records)
            
            # Send the converted data
            try:
                response = requests.post(url, json=json_records)
                print(f"Response status: {response}")
            except Exception as e:
                print(f"Error sending data: {e}")    
            
    except Exception as e:
        print("An error occurred:", e)
        return e

if __name__ == "__main__":

    # For testing purposes
    schedule.every().day.at("00:05").do(Post_job)
    while True:
        schedule.run_pending()
        time.sleep(15)
