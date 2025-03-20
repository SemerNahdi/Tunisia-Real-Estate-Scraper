from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient("mongodb://localhost:27017/", maxPoolSize=100)


async def get_db():
    db = client['tayara'] 
    collection = db['immo_neuf'] 
    
    await collection.create_index("metadata.publishedOn")
    await collection.create_index("price")
    await collection.create_index("location.governorate")
    
    return db
