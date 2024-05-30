import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.metrics import accuracy_score, classification_report
import requests
import os
from datetime import datetime, timedelta
import time
import csv

# Set environment variables
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

# Load the labeled data from the provided CSV file
labeled_data = pd.read_csv('news_with_relevance.csv')

# Ensure there are no NaN values in 'Headline' and 'Content' columns
labeled_data['Headline'].fillna('', inplace=True)
labeled_data['Content'].fillna('', inplace=True)

# Now check and drop any rows where 'Relevant' is NaN, if 'Relevant' is your target variable
labeled_data.dropna(subset=['Relevant'], inplace=True)

# Prepare the data
X = labeled_data['Headline'] + ' ' + labeled_data['Content']
y = labeled_data['Relevant']

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Create a text classification pipeline
pipeline = make_pipeline(TfidfVectorizer(), LogisticRegression())

# Train the model on the newly loaded data
pipeline.fit(X_train, y_train)

# Evaluate the model with the newly loaded data
y_pred = pipeline.predict(X_test)
print('Accuracy:', accuracy_score(y_test, y_pred))
print(classification_report(y_test, y_pred))

# Fetch and filter news and write to csv
def fetch_and_filter_news(start_date, end_date, batch_size=30):
    has_more = True
    request_count = 0
    max_requests_per_minute = 200
    minute_start_time = time.time()
    retry_attempts = 3  # Max retry after fail
    timeout_duration = 10

    articles = []

    current_end_date = end_date

    while has_more and current_end_date > start_date:
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
                        headers={
                            'APCA-API-KEY-ID': APCA_API_KEY_ID,
                            'APCA-API-SECRET-KEY': APCA_API_SECRET_KEY,
                        },
                        params=params,
                        timeout=timeout_duration
                    )
                    request_count += 1

                    if response.status_code == 200:
                        news_batch = response.json()
                        for article in news_batch['news']:
                            articles.append({
                                'Date': datetime.fromisoformat(article['created_at']).strftime('%Y-%m-%d'),
                                'Headline': article['headline'],
                                'Content': article['summary']
                            })
                            
                            article_text = article['headline'] + ' ' + article['summary']
                            relevant = pipeline.predict([article_text])[0]
                            if relevant == 1:
                                print("Relevant Article:")
                                print("Date:", article['created_at'])
                                print("Headline:", article['headline'])
                                print("Summary:", article['summary'])
                                print()

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

    # Classify and save relevant articles
    if articles:
        articles_df = pd.DataFrame(articles)
        articles_df['Relevant'] = pipeline.predict(articles_df['Headline'] + ' ' + articles_df['Content'])
        relevant_articles = articles_df[articles_df['Relevant'] == 1]
        relevant_articles = relevant_articles[['Date', 'Headline', 'Content']]
        relevant_articles.to_csv('relevant_macroeconomic_news_2.csv', index=False, columns=['Date', 'Headline', 'Content'])

start_date = datetime.now() - timedelta(days=365*3)
end_date = datetime.now()

fetch_and_filter_news(start_date, end_date, batch_size=30)
