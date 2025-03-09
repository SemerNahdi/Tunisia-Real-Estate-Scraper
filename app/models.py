 
from pydantic import BaseModel

class Annonce(BaseModel):
    id: str
    title: str
    description: str
    price: float
    phone: str
    delegation: str
    governorate: str
    publishedOn: str
    isModified: bool
    state: str
    subCategory: str
    isFeatured: bool
    publisher_name: str
    publisher_isShop: bool
    producttype: str
    image_urls: list