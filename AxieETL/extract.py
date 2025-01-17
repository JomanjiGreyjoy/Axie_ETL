import requests
import os
import json
from azure.storage.blob import BlobServiceClient
import logging
from datetime import datetime

def extract_data():
    """
    Calls the Axie API for 'recently sold' data and writes raw JSON to Azure Blob Storage.
    Returns the blob name for the newly written file.
    """
    api_key = os.getenv("AXIE_API_KEY", "")
    api_url = "https://api-gateway.skymavis.com/graphql/marketplace"
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": api_key
    }

    query = '''
    query GetRecentlySoldAxies {
    settledAuctions {
        axies(from: 0, size: 1000) {
        results {
        birthDate
        bodyShape
        class
        id
        offers(size: 100) {
            data {
            addedAt
            currentPriceUsd
            currentPrice
            endedPrice
            paymentToken
            endedAt
            basePrice
            startedAt
            status
            id
            }
        }
        parts {
            class
            name
            type
            abilities {
            attack
            defense
            energy
            description
            name
            id
            }
            id
        }
        primaryColor
        purity
        stage
        stats {
            hp
            morale
            skill
            speed
        }
        transferHistory(size: 100) {
            results {
            timestamp
            withPriceUsd
            withPrice
            txHash
            }
        }
        title
        order {
            addedAt
            currentPriceUsd
            basePrice
            currentPrice
            endedAt
            endedPrice
            paymentToken
            startedAt
            status
            id
        }
        pureness
        }
        }
    }
    }
    '''

    response = requests.post(api_url, headers=headers, json={"query": query}, timeout=10)
    response.raise_for_status()
    data = response.json()

    # Write to Blob
    blob_conn_str = os.getenv("BLOB_CONNECTION_STRING", "")
    blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)
    container_name = "axie-data"
    container_client = blob_service_client.get_container_client(container_name)

    # Create container if not exists
    try:
        container_client.create_container()
    except Exception:
        pass

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    blob_name = f"recently_sold_axies_{timestamp}.json"
    blob_client = container_client.get_blob_client(blob_name)
    blob_content = json.dumps(data, indent=2)
    blob_client.upload_blob(blob_content, overwrite=True)

    logging.info(f"Wrote raw JSON to {container_name}/{blob_name}.")
    return blob_name