import os
import requests
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from airflow.models import Variable
#from dotenv import load_dotenv
#import pandas as pd
#from azure.storage.blob import BlobServiceClient
#from datetime import datetime
# Load environment variables
#load_dotenv()

# Azure Blob Storage connection details
CONNECTION_STRING = Variable.get("AZURE_CONNECTION_STRING")
CONTAINER_NAME = Variable.get("AZURE_CONTAINER_NAME")
API_KEY = Variable.get("API_KEY")

SYMBOLS = ['AAPL', 'GOOG', 'MSFT', 'AMZN', 'IBM']

def extract_data(symbols, api_key):
    """
    Extract stock data from the API for a list of symbols.

    Args:
        symbols (list): List of stock symbols to extract data for.
        api_key (str): API key for authentication.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted stock data.
    """
    all_rows = []

    for symbol in symbols:
        print(f"Fetching data for {symbol}...")
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
        response = requests.get(url)
        data = response.json()

        # Extract time series data
        try:
            time_series = data['Time Series (Daily)']
        except KeyError:
            print(f"No data found for {symbol}. Skipping...")
            continue

        # Loop through the time series data
        for date, values in time_series.items():
            row = {
                "Date": datetime.strptime(date, "%Y-%m-%d"),
                "Symbol": symbol,
                "Open": float(values["1. open"]),
                "High": float(values["2. high"]),
                "Low": float(values["3. low"]),
                "Close": float(values["4. close"]),
                "Volume": int(values["5. volume"]),
            }
            all_rows.append(row)

    return pd.DataFrame(all_rows)

def transform_data(df):
    """
    Transform the extracted data by cleaning and deduplicating it.

    Args:
        df (pd.DataFrame): Raw extracted data.

    Returns:
        pd.DataFrame: Transformed data.
    """
    # Drop null values and duplicates
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    return df



def load_data_to_azure(df, connection_string, container_name):
    from azure.storage.blob import BlobServiceClient, BlobClient
    import pandas as pd
    from datetime import datetime
    import logging
    import io

    logging.basicConfig(level=logging.INFO)

    # Connect to Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Define the pattern for identifying previous files
    file_prefix = "stock_data_"
    file_extension = ".csv"
    previous_blob_name = None

    # Fetch the latest blob with matching pattern
    blobs = list(container_client.list_blobs())
    matching_blobs = [blob.name for blob in blobs if blob.name.startswith(file_prefix) and blob.name.endswith(file_extension)]

    if matching_blobs:
        previous_blob_name = max(matching_blobs)  # Get the latest file (lexicographical order works with timestamps)

    if previous_blob_name:
        try:
            logging.info(f"Previous data file found: {previous_blob_name}")
            
            # Download the existing blob content
            previous_blob_client = container_client.get_blob_client(previous_blob_name)
            blob_stream = io.BytesIO(previous_blob_client.download_blob().readall())
            previous_data = pd.read_csv(blob_stream)

            # Clean existing data
            previous_data["Date"] = pd.to_datetime(previous_data["Date"])
            previous_data.drop_duplicates(inplace=True)
            previous_data.dropna(inplace=True)

            # Concatenate with new data
            combined_data = pd.concat([previous_data, df]).drop_duplicates().reset_index(drop=True)

            # Rename the existing blob
            timestamp = datetime.now().strftime('%d%m%y_%H%M')
            backup_blob_name = f"{file_prefix}{timestamp}_backup{file_extension}"
            container_client.get_blob_client(previous_blob_name).start_copy_from_url(previous_blob_client.url)
            previous_blob_client.delete_blob()
            logging.info(f"Previous blob renamed to: {backup_blob_name}")
        
        except Exception as e:
            logging.error(f"Error processing previous blob: {e}")
            combined_data = df
    else:
        logging.info("No previous data file found. Creating a new one.")
        combined_data = df

    # Upload the new file
    current_timestamp = datetime.now().strftime('%d%m%y_%H%M')
    new_blob_name = f"{file_prefix}{current_timestamp}{file_extension}"

    # Save to a temporary buffer and upload
    output_stream = io.BytesIO()
    combined_data.to_csv(output_stream, index=False)
    output_stream.seek(0)

    try:
        new_blob_client = container_client.get_blob_client(new_blob_name)
        new_blob_client.upload_blob(output_stream, overwrite=True)
        logging.info(f"Data successfully uploaded to: {new_blob_name}")
    except Exception as e:
        logging.error(f"Error uploading new blob: {e}")


# Example of running the ETL process
if __name__ == "__main__":
    extracted_data = extract_data(SYMBOLS, API_KEY)
    transformed_data = transform_data(extracted_data)
    load_data_to_azure(transformed_data, CONNECTION_STRING, CONTAINER_NAME)

