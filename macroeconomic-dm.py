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
    next_page_token = None
    has_more = True
    request_count = 0
    max_requests_per_minute = 200
    minute_start_time = time.time()
    
    with open('macroeconomic_news.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Date", "Headline", "Content"])

        while start_date < end_date:
            batch_end_date = start_date + timedelta(days=30)
            while has_more:
                # Check if we've reached the rate limit
                if request_count >= max_requests_per_minute:
                    elapsed_time = time.time() - minute_start_time
                    if elapsed_time < 60:
                        time.sleep(60 - elapsed_time)
                    request_count = 0
                    minute_start_time = time.time()

                params = {
                    'start': start_date.strftime('%Y-%m-%d'),
                    'end': batch_end_date.strftime('%Y-%m-%d'),
                    'include_content': 'true',
                    'limit': batch_size,
                }

                if next_page_token:
                    params['page_token'] = next_page_token

                response = requests.get('https://data.alpaca.markets/v1beta1/news', headers=headers, params=params)
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
                    if not has_more:  # Reset for the next batch
                        start_date += timedelta(days=30)  # Next 30 days
                        next_page_token = None  # Reset pagination token
                        has_more = True  # Reset has_more
                else:
                    print("Failed to fetch news articles", response.status_code)
                    break

start_date = datetime.now() - timedelta(days=365*3)
end_date = datetime.now()

fetch_and_filter_news(start_date, end_date, batch_size=30)