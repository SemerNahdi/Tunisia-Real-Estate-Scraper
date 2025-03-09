from fastapi import FastAPI, HTTPException
from app.scraper import fetch_tayara_data
from pymongo import MongoClient

app = FastAPI()

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client['tayara']
collection = db['immo_neuf']

@app.get("/")
def read_root():
    """
    Root endpoint.
    """
    return {"message": "Welcome to the Tunisian Real Estate Scraping API!"}

@app.get("/annonces")
def get_annonces():
    """
    Endpoint to retrieve all collected announcements.
    """
    try:
        annonces = list(collection.find({}, {'_id': 0}))
        return {"annonces": annonces}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/scrape")
def scrape():
    """
    Endpoint to trigger a new scraping session.
    """
    try:
        fetch_tayara_data()  # Call your scraping function
        return {"status": "Scraping completed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)