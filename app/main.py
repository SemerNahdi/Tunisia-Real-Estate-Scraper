from fastapi import FastAPI, HTTPException, Query, status
from motor.motor_asyncio import AsyncIOMotorClient
from aiocache import cached
from aiocache.serializers import JsonSerializer
import logging
from datetime import datetime, timedelta
from datetime import datetime, timedelta, timezone
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from app.models import ListingRequest
from app.scraper import  fetch_tayara_data
from app.db import get_db
from typing import Optional
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
    allow_origins=["http://localhost:3000"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_db():
    global db, collection
    db = await get_db()  
    collection = db['immo_neuf']  

@app.get("/")
def read_root():
    """
    Root endpoint.
    """
    return {"message": "Welcome to the Tunisian Real Estate Scraping API!"}

@app.get("/annonces")
@cached(ttl=60, serializer=JsonSerializer())        
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

@app.get("/annonces/{listing_id}")
async def get_listing_by_id(listing_id: str):
    """Retrieve a specific listing by its unique identifier."""
    try:
        # Query MongoDB for the listing by the 'id' field
        listing = await collection.find_one({"id": listing_id}, {"_id": 0})
        
        if listing:
            return {"listing": listing}
        
        # If the listing is not found, raise a 404 error
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Listing not found"
        )
    except Exception as e:
        logger.error(f"Error fetching listing by ID: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while fetching the listing."
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

@app.get("/governorates-with-delegations")
async def get_governorates_with_delegations():
    """
    Retrieve a list of governorates along with their respective delegations.
    """
    try:
        # Aggregate data by governorate and delegation
        governorates_delegations = await collection.aggregate([
            {
                "$group": {
                    "_id": "$location.governorate",
                    "delegations": {"$addToSet": "$location.delegation"}
                }
            },
            {"$sort": {"_id": 1}}  # Sort by governorate name
        ]).to_list(length=None)

        # Format the result to a more friendly structure
        result = [{"governorate": item["_id"], "delegations": item["delegations"]} for item in governorates_delegations]

        return {"governorates_with_delegations": result}

    except Exception as e:
        logger.error(f"Error fetching governorates with delegations: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while fetching governorates and delegations."
        )

@app.post("/fetch-tayara-data/")
async def fetch_data(request: ListingRequest):
    """
    Endpoint to trigger the scraping process.
    """
    try:
        result = fetch_tayara_data(page=request.page, max_pages=request.max_pages)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
  
@app.get("/governorate-stats")
async def get_governorate_stats(
    governorate: str = Query(..., description="Governorate of interest"),
    start_date: datetime = Query(..., description="Start date for the time period"),
    end_date: datetime = Query(..., description="End date for the time period")
):
    """
    Retrieve statistics for a specific governorate within a given time period.
    Includes total listings (split by rent/sale), average prices, monthly average prices,
    custom time range average prices, listings by delegation, and publisher type distribution.
    """
    try:
        # Base query for the governorate and date range
        base_query = {
            "location.governorate": governorate,
            "metadata.publishedOn": {"$gte": start_date.isoformat(), "$lte": end_date.isoformat()}
        }

        # 1. Total number of listings (split by rent and sale)
        total_listings_rent = await collection.count_documents({**base_query, "metadata.producttype": 0})
        total_listings_sale = await collection.count_documents({**base_query, "metadata.producttype": 1})

        # 2. Average rental and sale prices
        avg_price_rent_result = await collection.aggregate([
            {"$match": {**base_query, "metadata.producttype": 0, "price": {"$gt": 0, "$lt": 10_000, "$exists": True}}},
            {"$group": {"_id": None, "avg_price": {"$avg": "$price"}}}
        ]).to_list(length=None)
        avg_price_rent = avg_price_rent_result[0]["avg_price"] if avg_price_rent_result else 0

        avg_price_sale_result = await collection.aggregate([
            {"$match": {**base_query, "metadata.producttype": 1, "price": {"$gt": 0, "$lt": 1_000_000, "$exists": True}}},
            {"$group": {"_id": None, "avg_price": {"$avg": "$price"}}}
        ]).to_list(length=None)
        avg_price_sale = avg_price_sale_result[0]["avg_price"] if avg_price_sale_result else 0

        # 3. Monthly average prices for the period (for line graph)
        monthly_avg_prices = await collection.aggregate([
            {"$match": {**base_query, "price": {"$gt": 0, "$exists": True}}},
            {
                "$group": {
                    "_id": {
                        "year": {"$year": {"$dateFromString": {"dateString": "$metadata.publishedOn"}}},
                        "month": {"$month": {"$dateFromString": {"dateString": "$metadata.publishedOn"}}},
                        "producttype": "$metadata.producttype"
                    },
                    "avg_price": {"$avg": "$price"},
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"_id.year": 1, "_id.month": 1}}
        ]).to_list(length=None)

        monthly_avg_prices_formatted = [
            {
                "year": item["_id"]["year"],
                "month": item["_id"]["month"],
                "producttype": "rent" if item["_id"]["producttype"] == 0 else "sale",
                "avg_price": item["avg_price"],
                "count": item["count"]
            } for item in monthly_avg_prices
        ]

        # 4. Average prices for a custom time range (by months) for rent or sale
        custom_range_avg_prices = await collection.aggregate([
            {"$match": {**base_query, "price": {"$gt": 0, "$exists": True}}},
            {
                "$group": {
                    "_id": {
                        "year": {"$year": {"$dateFromString": {"dateString": "$metadata.publishedOn"}}},
                        "month": {"$month": {"$dateFromString": {"dateString": "$metadata.publishedOn"}}},
                        "producttype": "$metadata.producttype"
                    },
                    "avg_price": {"$avg": "$price"}
                }
            },
            {"$sort": {"_id.year": 1, "_id.month": 1}}
        ]).to_list(length=None)

        custom_range_avg_prices_formatted = [
            {
                "year": item["_id"]["year"],
                "month": item["_id"]["month"],
                "producttype": "rent" if item["_id"]["producttype"] == 0 else "sale",
                "avg_price": item["avg_price"]
            } for item in custom_range_avg_prices
        ]

        # 5. Number of listings by delegation within the governorate
        delegation_stats = await collection.aggregate([
            {"$match": base_query},
            {"$group": {"_id": "$location.delegation", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]).to_list(length=None)

        # 6. Distribution of listings by publisher type (shop vs. individual)
        publisher_stats = await collection.aggregate([
            {"$match": base_query},
            {"$group": {"_id": "$metadata.publisher.isShop", "count": {"$sum": 1}}}
        ]).to_list(length=None)

        publisher_stats_formatted = [
            {"publisher_type": "shop" if item["_id"] else "individual", "count": item["count"]}
            for item in publisher_stats
        ]

        # Return the combined statistics
        return {
            "governorate": governorate,
            "time_period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            "total_listings": {
                "rent": total_listings_rent,
                "sale": total_listings_sale,
                "total": total_listings_rent + total_listings_sale
            },
            "average_prices": {
                "rent": avg_price_rent,
                "sale": avg_price_sale
            },
            "monthly_average_prices": monthly_avg_prices_formatted,
            "custom_range_average_prices": custom_range_avg_prices_formatted,
            "delegation_stats": delegation_stats,
            "publisher_stats": publisher_stats_formatted
        }

    except Exception as e:
        logger.error(f"Error fetching governorate statistics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while fetching governorate statistics."
        )



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)