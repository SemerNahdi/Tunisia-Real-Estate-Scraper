from motor.motor_asyncio import AsyncIOMotorClient

# Initialize MongoDB connection with connection pooling
client = AsyncIOMotorClient("mongodb://localhost:27017/", maxPoolSize=100)

def get_db():
    db = client['tayara']
    return db