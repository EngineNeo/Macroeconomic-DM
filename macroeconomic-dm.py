import requests
import csv
from datetime import datetime, timedelta
import os
import time

# Set env variables
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

# Keywords related to macroeconomic indicators
keywords = [
    "Gross Domestic Product", "GDP", "Unemployment Rate", "Inflation Rate",
    "Consumer Price Index", "CPI", "Producer Price Index", "PPI", "Interest Rates",
    "Balance of Trade", "Government Debt", "Budget Deficit", "Surplus", "Exchange Rates",
    "Money Supply", "Industrial Production", "Retail Sales", "Housing Starts"
]

# Prepare headers for the HTTP request
headers = {
    'APCA-API-KEY-ID': APCA_API_KEY_ID,
    'APCA-API-SECRET-KEY': APCA_API_SECRET_KEY,
}

# Fetch and filter news and write to csv
def fetch_and_filter_news(start_date, end_date, batch_size=30):
    has_more = True
    request_count = 0
    max_requests_per_minute = 200
    minute_start_time = time.time()
    retry_attempts = 3  # Max retry after fail
    timeout_duration = 10

    with open('macroeconomic_news.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Date", "Headline", "Content"])

        current_end_date = end_date

        while current_end_date > start_date:
            batch_start_date = current_end_date - timedelta(days=30)
            has_more = True  # Reset has_more for the new batch
            next_page_token = None  # Reset pagination token for the new batch

            while has_more:
                # Check rate limit
                if request_count >= max_requests_per_minute:
                    elapsed_time = time.time() - minute_start_time
                    if elapsed_time < 60:
                        time.sleep(60 - elapsed_time)
                    request_count = 0
                    minute_start_time = time.time()

                params = {
                    'start': batch_start_date.strftime('%Y-%m-%d'),
                    'end': current_end_date.strftime('%Y-%m-%d'),
                    'include_content': 'true',
                    'limit': batch_size,
                }

                if next_page_token:
                    params['page_token'] = next_page_token

                for attempt in range(retry_attempts):
                    try:
                        response = requests.get(
                            'https://data.alpaca.markets/v1beta1/news',
                            headers=headers,
                            params=params,
                            timeout=timeout_duration
                        )
                        request_count += 1

                        if response.status_code == 200:
                            news_batch = response.json()
                            for article in news_batch['news']:
                                # Keyword check
                                if any(keyword in article['headline'] or keyword in article['summary'] for keyword in keywords):
                                    date = datetime.fromisoformat(article['created_at']).strftime('%m/%d/%Y')
                                    headline = article['headline']
                                    content = article['summary']
                                    writer.writerow([date, headline, content])
                                    print(date, headline, content)

                            next_page_token = news_batch.get('next_page_token')
                            has_more = next_page_token is not None
                            break  # Successful fetch, break retry loop
                        else:
                            print(f"Failed to fetch news articles {response.status_code} on attempt {attempt + 1}")
                            if response.status_code == 400:
                                print("Bad request, skipping this batch.")
                                has_more = False  # Prevent retry for this batch
                                break  # Exit retry loop
                            time.sleep(2 ** attempt)  # Exponential backoff
                    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                        print(f"Connection error on attempt {attempt + 1}: {e}")
                        time.sleep(2 ** attempt)  # Exponential backoff

                if not has_more:
                    # In case of no success for fetch move to next batch
                    current_end_date -= timedelta(days=30)

start_date = datetime.now() - timedelta(days=365*5)
end_date = datetime.now()

fetch_and_filter_news(start_date, end_date, batch_size=30)