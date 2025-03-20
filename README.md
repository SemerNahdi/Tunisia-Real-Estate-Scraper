# **Tunisian Real Estate Scraping API**

This project is a **FastAPI-based web scraping solution** for extracting real estate listings (e.g., properties for sale or rent) from Tunisian websites like Tayara.tn. The scraped data is stored in a **MongoDB database** and exposed via a **RESTful API**.

## **Features**

### **1. Web Scraping**

- Extracts real estate listings from Tayara.tn.
- Handles pagination and saves data to MongoDB.
- Stores data in MongoDB.

### **2. RESTful API**

- Exposes the scraped data via endpoints.
- Allows triggering new scraping sessions.
- Allows searching, filtering, and sorting real estate listings.
- Supports pagination for retrieving listings in smaller chunks.

### **3. Data Storage**

- Stores scraped data in MongoDB.
- Supports exporting data to JSON and CSV files.

### **4. Automatic Documentation**

- Generates interactive API documentation using Swagger UI and ReDoc.

### **5. Caching**

- Uses aiocache to cache API responses for improved performance.

### **6. Error Handling & Logging**

- Includes robust error handling for failed requests.
- Logs errors for debugging.


## **Technologies Used**

- **Python**: Programming language.
- **FastAPI**: Web framework for building the API.
- **Uvicorn**: ASGI server to run FastAPI.
- **MongoDB**: NoSQL database for storing scraped data.
- **Pymongo**: Python driver for MongoDB.
- **Requests**: HTTP library for making requests.
- **aiocache**: Caching library for FastAPI.
- **Pandas**: Data manipulation library for exporting data to CSV.

## **Installation**

### **Prerequisites**

1. **Python 3.7+**: Ensure Python is installed on your system.
2. **MongoDB**: Install and run MongoDB locally or use a cloud service like MongoDB Atlas.

### **Steps**

1. Clone the repository:

   ```bash
   git clone https://github.com/SemerNahdi/Tunisia-Real-Estate-Scraper.git
   cd Tunisia-Real-Estate-Scraper
   ```

2. Create a virtual environment:

   ```bash
   python -m venv .venv
   venv\Scripts\activate
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Start MongoDB (ensure it is running locally on `mongodb://localhost:27017`).

5. Run the FastAPI application:

   ```bash
   uvicorn app.main:app --reload
   ```

6. Access the API at `http://127.0.0.1:8000`.

## **API Endpoints**

### **1. Root Endpoint**

- **URL**: `http://127.0.0.1:8000`
- **Method**: `GET`
- **Description**: Welcome message.
- **Response**:
  ```json
  {
    "message": "Welcome to the Tunisian Real Estate Scraping API!"
  }
  ```

### **2. Get All Listings**

- **URL**: `http://127.0.0.1:8000/annonces`
- **Method**: `GET`
- **Description**: Retrieve all real estate listings from the database.
- **Parameters**:
    - skip (optional): Number of items to skip (default: 0).
    - limit (optional): Number of items to return (default: 10).
- **Response**:

```json
{
  "annonces": [
    {
      "id": "12345",
      "title": "Beautiful Apartment for Sale",
      "description": "3 bedrooms, 2 bathrooms...",
      "price": 250000,
      "phone": "12345678",
      "delegation": "Tunis",
      "governorate": "Tunis",
      "publishedOn": "2025-01-01",
      "isModified": false,
      "state": "new",
      "subCategory": "Apartment",
      "isFeatured": true,
      "publisher_name": "John Doe",
      "publisher_isShop": false,
      "producttype": "sale",
      "image_urls": ["https://example.com/image1.jpg"]
    }
  ],
  "total": 1000,
  "skip": 0,
  "limit": 10
}
```

### **3. Trigger Scraping**

- **URL**: `http://127.0.0.1:8000/scrape`
- **Method**: `POST`
- **Description**: Trigger a new scraping session to update the database.
- **Response**:

```json
{
  "status": "Scraping completed successfully"
}
```

### **4. Retrieve Listings by Date Range**
- **URL**: `http://127.0.0.1:8000/annonces/date`
- **Method**: `GET`
- **Description**: Retrieve announcement based on a time limit.
- **Parameters**:
  - start_date (required).
  - end_date (required)
  - producttype (optional, 1 for sale, 0 for rent)

### **5.  Retrieve Listings by Location**
- **URL**: `http://127.0.0.1:8000/annonces/location`
- **Method**: `GET`
- **Description**: Retrieve announcement based on a time limit.
- **Parameters**:
    - governorate (required)
    - delegation  (optional)
    - producttype (optional, 1 for sale, 0 for rent)


### **6. Retrieve Statistics**
- **URL**: `http://127.0.0.1:8000//statistics`
- **Method**: `GET`
- **Description**: Retrieve statistics for dashboards.
- **Includes**:
    - Total listings
    - Listings by governorate
    - Listings by type (sale/rent)
    - Average price for sale/rent
    - Listings by publisher type (shop/individual)
      


## **Project Structure**

```
📂 Tunisia-Real-Estate-Scraper
    📂 app
        📄 init.py
        📄 main.py
        📄 scraper.py
        📄 models.py
        📄 db.py
    📄 requirements.txt
    📄 README.md

```

## **Usage**

### **1. Start the API**

Run the FastAPI application:

```bash
uvicorn app.main:app --reload
```

### **2. Access the API**

#### **Interactive API Documentation**

The API includes **automatic interactive documentation** powered by Swagger UI and ReDoc. You can access it at the following URLs:

1. **Swagger UI**: `http://127.0.0.1:8000/docs`
2. **ReDoc**: `http://127.0.0.1:8000/redoc`
3. **Root Endpoint**: `http://127.0.0.1:8000/`

#### **Example Usage**

1. **Access the Root Endpoint**:

   ```bash
   curl http://127.0.0.1:8000/
   ```

2. **Retrieve All Listings**:

   ```bash
   curl "http://127.0.0.1:8000/annonces?skip=20&limit=10"
   ```

3. **Trigger a New Scraping Session**:
   ```bash
   curl -X POST http://127.0.0.1:8000/scrape
   ```

## **Data Storage**

- **MongoDB**:
  - Database: `tayara`
  - Collection: `immo_neuf`

- **Files**:
  - Raw data: `tayara_immo_neuf_raw.json`
  - Structured data: `tayara_immo_neuf_structured.csv`

## **Caching**
   - The /annonces endpoint caches responses for 60 seconds using aiocache.
   - Caching reduces database load and improves API performance.

## **Error Handling**
   - The API includes robust error handling and logging.
   - Errors are logged for debugging, and meaningful error messages are returned to the client.

## **License**

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
