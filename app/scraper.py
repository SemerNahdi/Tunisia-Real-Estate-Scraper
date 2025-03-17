import http.client
import json
import time
import logging
import httpx
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
from app.db import get_db

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_tayara_data_sync():
    """
    Synchronous version of the scraping function.
    """
    conn = http.client.HTTPSConnection("www.tayara.tn")
    headers = {
        'cookie': "caravel-cookie=%220ca08badccbd001c%22",
        'User-Agent': "insomnia/10.3.1"
    }
    page = 1
    all_listings = []
    total_listings_count = 0
    max_pages = 500  # Set a reasonable maximum page limit

    try:
        while page <= max_pages:
            payload = ""
            url = f"/_next/data/TGf_Z4p6UhZCJW0Sjbrbt/en/ads/c/Immobilier.json?page={page}&category=Immobilier"
            conn.request("GET", url, payload, headers)
            res = conn.getresponse()
            status_code = res.status
            data = res.read().decode('utf-8')

            if status_code != 200:
                logger.error(f"Error: HTTP status code {status_code} on page {page}")
                logger.error(f"Response content: {data}")
                break  # Stop the loop on HTTP error

            try:
                json_data = json.loads(data)
                listings = json_data['pageProps']['searchedListingsAction']['newHits']

                if not listings:
                    logger.info(f"No listings found on page {page}. Assuming end of pages.")
                    break  # Stop the loop if no listings are found

                golden_listing = json_data['pageProps'].get('goldenListing')
                premium_listing = json_data['pageProps']['searchedListingsAction'].get('premiumHits')

                all_listings.extend(listings)
                if golden_listing:
                    all_listings.append(golden_listing)
                if premium_listing:
                    all_listings.extend(premium_listing)

                total_listings_count += len(listings)
                logger.info(f"Page {page} processed. Listings: {len(listings)}, Total: {total_listings_count}")
                page += 1
                time.sleep(1)  # Pause for 1 second between requests

            except json.JSONDecodeError:
                logger.error(f"Error: Invalid JSON on page {page}")
                logger.error(f"Response content: {data}")
                break  # Stop the loop if JSON is invalid

    except Exception as e:
        logger.error(f"Error occurred: {e}")

    finally:
        conn.close()

    # Save raw data to JSON file
    with open('tayara_immo_neuf_raw.json', 'w', encoding='utf-8') as f:
        json.dump(all_listings, f, ensure_ascii=False, indent=4)

    # Save data to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client['tayara']
    collection = db['immo_neuf']
    collection.delete_many({})  # Clear existing data
    collection.insert_many(all_listings)  # Insert new data

    logger.info(f"\nAll pages processed. Total listings found: {total_listings_count}")
    logger.info(f"Total listings imported into MongoDB: {len(all_listings)}")

from pymongo import UpdateOne

async def fetch_tayara_data_async():
    """
    Optimized asynchronous scraping function that inserts only new listings.
    Returns the number of new listings inserted into MongoDB.
    """
    db = get_db()
    collection = db['immo_neuf']

    async with httpx.AsyncClient() as client:
        page = 1
        all_listings = []
        existing_ids = set()  # We'll use MongoDB queries to check for duplicates

        while True:
            url = f"https://www.tayara.tn/_next/data/TGf_Z4p6UhZCJW0Sjbrbt/en/ads/c/Immobilier.json?page={page}&category=Immobilier"
            response = await client.get(url)

            if response.status_code != 200:
                logger.error(f"Error: HTTP status code {response.status_code} on page {page}")
                break

            try:
                data = response.json()
                listings = data['pageProps']['searchedListingsAction']['newHits']
                if not listings:
                    logger.info(f"No listings found on page {page}. Assuming end of pages.")
                    break

                # Collect IDs and check if they exist in the database
                new_listings = []
                ids_to_check = [listing['id'] for listing in listings]

                # Query MongoDB for existing listings by IDs
                existing = await collection.find({"id": {"$in": ids_to_check}}).to_list(length=None)
                existing_ids_set = {entry['id'] for entry in existing}

                # Filter out listings that already exist in the DB
                for listing in listings:
                    if listing['id'] not in existing_ids_set:
                        new_listings.append(listing)

                if new_listings:
                    all_listings.extend(new_listings)
                    logger.info(f"Page {page} processed. New listings: {len(new_listings)}, Total new: {len(all_listings)}")
                else:
                    logger.info(f"Page {page} processed. No new listings.")

                page += 1

            except Exception as e:
                logger.error(f"Error processing page {page}: {e}")
                break

        # Bulk insert new listings into MongoDB if any
        if all_listings:
            try:
                # Bulk insert operation to add only new listings
                operations = [UpdateOne({"id": listing['id']}, {"$set": listing}, upsert=True) for listing in all_listings]

                # Perform the bulk operation
                if operations:
                    result = await collection.bulk_write(operations)
                    inserted_count = result.upserted_count  # Number of new listings inserted
                    logger.info(f"Bulk operation completed. Inserted {inserted_count} new listings.")
                    return inserted_count  # Return the number of new listings inserted
                else:
                    logger.info("No new listings to insert.")
                    return 0  # No new listings to insert
            except Exception as e:
                logger.error(f"Error performing bulk insert into MongoDB: {e}")
                return 0  # Return 0 if an error occurs
        else:
            logger.warning("No new listings found to import.")
            return 0  # Return 0 if no new listings were found

# Choose which version to use (sync or async)
fetch_tayara_data = fetch_tayara_data_async  # Default to async