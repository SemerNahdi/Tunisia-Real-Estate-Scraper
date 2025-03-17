from fastapi import FastAPI, HTTPException, Query, status
from motor.motor_asyncio import AsyncIOMotorClient
from aiocache import cached
from aiocache.serializers import JsonSerializer
import logging
from datetime import datetime, timedelta
from datetime import datetime, timedelta, timezone
from fastapi.middleware.cors import CORSMiddleware
from app.scraper import fetch_tayara_data

# Set up logging to log to a file
log_file = 'app.log'  # Specify the log file location
logger = logging.getLogger()

# Configure the logging to write logs to the file
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.DEBUG)  # Change to DEBUG for detailed logging
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Allow your Next.js frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize MongoDB connection with connection pooling
client = AsyncIOMotorClient("mongodb://localhost:27017/", maxPoolSize=100)
db = client['tayara']
collection = db['immo_neuf']

@app.get("/")
def read_root():
    """
    Root endpoint.
    """
    return {"message": "Welcome to the Tunisian Real Estate Scraping API!"}

@app.get("/annonces")
@cached(ttl=60, serializer=JsonSerializer())  # Cache for 60 seconds
async def get_annonces(
    skip: int = Query(0, description="Number of items to skip"),
    limit: int = Query(10, description="Number of items to return")
):
    """
    Retrieve paginated real estate listings.
    """
    try:
        annonces = await collection.find({}, {'_id': 0}).skip(skip).limit(limit).to_list(length=limit)
        total = await collection.count_documents({})
        return {
            "annonces": annonces,
            "total": total,
            "skip": skip,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error fetching listings: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while fetching listings."
        )


@app.get("/annonces/new")
async def get_new_annonces():
    """
    Retrieve new real estate listings added in the last 24 hours.
    """
    try:
        # Calculate the timestamp for 24 hours ago with a timezone (UTC)
        twenty_four_hours_ago = datetime.now(timezone.utc) - timedelta(hours=24)
        logger.debug(f"Timestamp for 24 hours ago: {twenty_four_hours_ago}")

        # Convert the timestamp to string in ISO format (the same format as 'metadata.publishedOn')
        twenty_four_hours_ago_str = twenty_four_hours_ago.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        logger.debug(f"Formatted timestamp for query: {twenty_four_hours_ago_str}")
        # Query MongoDB for listings published in the last 24 hours
        query = {
    "metadata.publishedOn": {"$gte": twenty_four_hours_ago_str}
}

        logger.debug(f"MongoDB query: {query}")

        # Fetch new listings
        new_annonces = await collection.find(query, {'_id': 0}).to_list(length=None)
        count = len(new_annonces)
        logger.debug(f"Found {count} new listings")

        if count == 0:
            logger.info("No new listings found within the last 24 hours.")

        return {
            "new_annonces": new_annonces,
            "count": count
        }
    except Exception as e:
        logger.error(f"Error fetching new listings: {e}")
        raise HTTPException(
            status_code=500,
            detail="An error occurred while fetching new listings."
        )

        
@app.get("/statistics")
async def get_statistics():
    """
    Retrieve statistics for building a dashboard.
    """
    try:
        # Total number of listings
        total_listings = await collection.count_documents({})

        # Number of listings by governorate
        governorate_stats = await collection.aggregate([
            {"$group": {"_id": "$location.governorate", "count": {"$sum": 1}}}
        ]).to_list(length=None)

        # Number of listings by type (sale/rent)
        type_stats = await collection.aggregate([
            {"$group": {"_id": "$metadata.producttype", "count": {"$sum": 1}}}
        ]).to_list(length=None)

        # Average price for sale listings (producttype = 1)
        avg_price_sale_result = await collection.aggregate([
            {"$match": {"metadata.producttype": 1, "price": {"$gt": 0, "$lt": 1_000_000, "$exists": True}}},
            {"$group": {"_id": None, "avg_price": {"$avg": "$price"}}}
        ]).to_list(length=None)
        avg_price_sale = avg_price_sale_result[0]["avg_price"] if avg_price_sale_result else 0

        # Average price for rent listings (producttype = 0)
        avg_price_rent_result = await collection.aggregate([
            {"$match": {"metadata.producttype": 0, "price": {"$gt": 0, "$lt": 10_000, "$exists": True}}},
            {"$group": {"_id": None, "avg_price": {"$avg": "$price"}}}
        ]).to_list(length=None)
        avg_price_rent = avg_price_rent_result[0]["avg_price"] if avg_price_rent_result else 0

        # Number of listings by publisher type (shop vs. individual)
        publisher_stats = await collection.aggregate([
            {"$group": {"_id": "$metadata.publisher.isShop", "count": {"$sum": 1}}}
        ]).to_list(length=None)

        # Number of listings by delegation, grouped by governorate
        delegation_by_governorate = await collection.aggregate([
            {
                "$group": {
                    "_id": {
                        "governorate": "$location.governorate",
                        "delegation": "$location.delegation"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": "$_id.governorate",
                    "delegations": {
                        "$push": {
                            "delegation": "$_id.delegation",
                            "count": "$count"
                        }
                    }
                }
            },
            {"$sort": {"_id": 1}}  # Sort governorates alphabetically
        ]).to_list(length=None)

        return {
            "total_listings": total_listings,
            "governorate_stats": governorate_stats,
            "type_stats": type_stats,
            "avg_price_sale": avg_price_sale,
            "avg_price_rent": avg_price_rent,
            "publisher_stats": publisher_stats,
            "delegation_by_governorate": delegation_by_governorate
        }
    except Exception as e:
        logger.error(f"Error fetching statistics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while fetching statistics."
        )

@app.get("/annonces/price")
async def get_annonces_by_price(
    min_price: int = Query(0, description="Minimum price"),
    max_price: int = Query(1_000_000, description="Maximum price"),
    producttype: int = Query(None, description="1 for sale, 0 for rent"),
    skip: int = Query(0, description="Number of items to skip"),
    limit: int = Query(10, description="Number of items to return")
):
    """
    Retrieve real estate listings by a specified price range.
    """
    try:
        query = {"price": {"$gte": min_price, "$lte": max_price}}
        if producttype is not None:
            query["metadata.producttype"] = producttype

        annonces = await collection.find(query, {'_id': 0}).skip(skip).limit(limit).to_list(length=limit)
        total = await collection.count_documents(query)

        return {
            "annonces": annonces,
            "total": total,
            "skip": skip,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error fetching listings by price range: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while fetching listings."
        )

@app.get("/annonces/date")
async def get_annonces_by_date(
    start_date: datetime = Query(..., description="Start date for the range"),
    end_date: datetime = Query(..., description="End date for the range"),
    producttype: int = Query(None, description="1 for sale, 0 for rent"),
    skip: int = Query(0, description="Number of items to skip"),
    limit: int = Query(10, description="Number of items to return")
):
    """
    Retrieve real estate listings within a specific date range.
    """
    try:
        query = {
            "metadata.publishedOn": {"$gte": start_date.isoformat(), "$lte": end_date.isoformat()}
        }
        if producttype is not None:
            query["metadata.producttype"] = producttype

        annonces = await collection.find(query, {'_id': 0}).skip(skip).limit(limit).to_list(length=limit)
        total = await collection.count_documents(query)

        return {
            "annonces": annonces,
            "total": total,
            "skip": skip,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error fetching listings by date range: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while fetching listings."
        )

@app.get("/annonces/location")
async def get_annonces_by_location(
    governorate: str = Query(..., description="Governorate of interest"),
    delegation: str = Query(None, description="Delegation of interest"),
    producttype: int = Query(None, description="1 for sale, 0 for rent"),
    skip: int = Query(0, description="Number of items to skip"),
    limit: int = Query(10, description="Number of items to return")
):
    """
    Retrieve real estate listings filtered by location (governorate and optionally by delegation) and product type.
    """
    try:
        query = {"location.governorate": governorate}
        if delegation:
            query["location.delegation"] = delegation
        if producttype is not None:
            query["metadata.producttype"] = producttype

        annonces = await collection.find(query, {'_id': 0}).skip(skip).limit(limit).to_list(length=limit)
        total = await collection.count_documents(query)

        return {
            "annonces": annonces,
            "total": total,
            "skip": skip,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error fetching listings by location: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while fetching listings."
        )

@app.post("/scrape")
async def scrape():
    """
    Trigger a new scraping session.
    """
    try:
        # Call your scraping function (fetch_tayara_data)
        
        new_announcements=  await fetch_tayara_data()
        return {"status": "Scraping completed successfully" , "new_announcements":new_announcements}
    except Exception as e:
        logger.error(f"Error during scraping: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during scraping."
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)