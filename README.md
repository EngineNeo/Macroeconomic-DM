## Macroeconomic-DM

This Python script fetches macroeconomic news articles from the Alpaca News API, filters them based on specific keywords, and saves the relevant articles to a CSV file. The script ensures compliance with rate limits and includes retry logic for handling intermittent API failures.

### Prerequisites

- Python 3.6 or higher
- Requests library
- Environment variables for Alpaca API credentials

### Environment Setup

Ensure you have the following environment variables set:

- `APCA_API_KEY_ID`: Your Alpaca API Key ID
- `APCA_API_SECRET_KEY`: Your Alpaca API Secret Key

You can set these environment variables in your system or in a `.env` file in the root of your project.

### Installation

1. Clone the repository or download the script.
2. Install the required dependencies: