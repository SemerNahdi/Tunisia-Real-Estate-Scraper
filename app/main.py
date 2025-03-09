from fastapi import FastAPI, HTTPException, Query, status
from motor.motor_asyncio import AsyncIOMotorClient
from aiocache import cached
from aiocache.serializers import JsonSerializer
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

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

@app.post("/scrape")
async def scrape():
    """
    Trigger a new scraping session.
    """
    try:
        # Call your scraping function (fetch_tayara_data)
        from app.scraper import fetch_tayara_data
        await fetch_tayara_data()
        return {"status": "Scraping completed successfully"}
    except Exception as e:
        logger.error(f"Error during scraping: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during scraping."
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)