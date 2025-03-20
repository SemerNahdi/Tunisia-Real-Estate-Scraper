import http.client
import json
import time
import logging
from turtle import pd
import httpx
from pymongo import MongoClient, UpdateOne

from app.db import get_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_tayara_data(page: int = 1, max_pages: int = 317):
    """
    Synchronously fetch data from Tayara and store it in MongoDB.
    Returns the number of listings processed and the listings themselves.
    """
    conn = http.client.HTTPSConnection("www.tayara.tn")
    headers = {
        'cookie': "caravel-cookie=%220ca08badccbd001c%22",
        'User-Agent': "insomnia/10.3.1"
    }
    all_listings = []
    total_listings_count = 0

    try:
        while page <= max_pages:
            url = f"/_next/data/TGf_Z4p6UhZCJW0Sjbrbt/en/ads/c/Immobilier.json?page={page}&category=Immobilier"
            conn.request("GET", url, "", headers)
            res = conn.getresponse()
            status_code = res.status
            data = res.read().decode('utf-8')

            if status_code != 200:
                logger.error(f"Error: HTTP status code {status_code} on page {page}")
                break

            try:
                json_data = json.loads(data)
                listings = json_data['pageProps']['searchedListingsAction']['newHits']

                if not listings:
                    logger.info(f"No listings found on page {page}. Assuming end of pages.")
                    break

                # Process listings
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
                time.sleep(1)  # Be respectful of the website's server by adding a delay between requests

            except json.JSONDecodeError:
                logger.error(f"Error: Invalid JSON on page {page}")
                break

    except Exception as e:
        logger.error(f"Error occurred: {e}")
    finally:
        conn.close()

    # Save to MongoDB
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client['tayara']
        collection = db['immo_neuf']
        collection.delete_many({})  # Clear existing data
        collection.insert_many(all_listings)  # Insert new data
        logger.info(f"Data successfully saved to MongoDB.")
    except Exception as e:
        logger.error(f"Error saving data to MongoDB: {e}")

    # Save structured CSV (ensure the save_to_csv function is defined elsewhere)
    try:
        save_to_csv(all_listings)  
        logger.info(f"Data successfully saved to CSV.")
    except Exception as e:
        logger.error(f"Error saving data to CSV: {e}")

    return {
        "message": f"Successfully processed {total_listings_count} listings.",
        "total_listings_processed": total_listings_count
    }

def save_to_csv(listings):
    """
    Converts listing data to a structured CSV format.
    """
    csv_data = []
    for listing in listings:
        csv_data.append({
            'id': listing['id'],
            'title': listing['title'],
            'description': listing['description'],
            'price': listing.get('price', ''),
            'phone': listing.get('phone', ''),
            'delegation': listing['location']['delegation'],
            'governorate': listing['location']['governorate'],
            'publishedOn': listing['metadata']['publishedOn'],
            'isModified': listing['metadata']['isModified'],
            'state': listing['metadata']['state'],
            'subCategory': listing['metadata']['subCategory'],
            'isFeatured': listing['metadata']['isFeatured'],
            'publisher_name': listing['metadata']['publisher']['name'],
            'publisher_isShop': listing['metadata']['publisher']['isShop'],
            'producttype': listing['metadata']['producttype'],
            'image_urls': listing['images']
        })
    
    df = pd.DataFrame(csv_data)
    df.to_csv('tayara_immo_neuf_structured.csv', index=False, encoding='utf-8-sig')
    logger.info("Data has been saved to tayara_immo_neuf_structured.csv")

### Asynchronous Version
async def fetch_tayara_data_async():
    """
    Optimized asynchronous scraping function that inserts only new listings.
    Returns the number of new listings inserted into MongoDB.
    """
    db = await get_db()
    collection = db['immo_neuf']

    async with httpx.AsyncClient() as client:
        page = 1
        all_listings = []
        existing_ids = set()

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

                # Process listings
                new_listings = []
                ids_to_check = [listing['id'] for listing in listings]

                existing = await collection.find({"id": {"$in": ids_to_check}}).to_list(length=None)
                existing_ids_set = {entry['id'] for entry in existing}

                # Filter out existing listings
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

        if all_listings:
            try:
                # Perform bulk insert operation
                operations = [UpdateOne({"id": listing['id']}, {"$set": listing}, upsert=True) for listing in all_listings]
                result = await collection.bulk_write(operations)
                inserted_count = result.upserted_count
                logger.info(f"Bulk operation completed. Inserted {inserted_count} new listings.")
                return inserted_count
            except Exception as e:
                logger.error(f"Error performing bulk insert: {e}")
                return 0
        else:
            logger.warning("No new listings found to import.")
            return 0
