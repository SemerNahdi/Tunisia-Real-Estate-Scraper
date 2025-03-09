import http.client
import http.client
import json
import pandas as pd
from pymongo import MongoClient

conn = http.client.HTTPSConnection("www.tayara.tn")

payload = ""

headers = {
    'cookie': "caravel-cookie=%220ca08badccbd001c%22",
    'User-Agent': "insomnia/10.3.1"
    }

conn.request("GET", "/_next/data/TGf_Z4p6UhZCJW0Sjbrbt/en/ads/c/Immobilier.json?page=100&category=Immobilier", payload, headers)

try:
    res = conn.getresponse()
    data = res.read().decode('utf-8')
    json_data = json.loads(data)
    listings = json_data['pageProps']['searchedListingsAction']['newHits']
    golden_listing = json_data['pageProps'].get('goldenListing')

    # Sauvegarde des données brutes dans un fichier JSON
    all_listings = listings.copy()
    if golden_listing:
        all_listings.append(golden_listing)

    with open('tayara_immo_neuf_raw.json', 'w', encoding='utf-8') as f:
        json.dump(all_listings, f, ensure_ascii=False, indent=4)

    # Chargement du fichier JSON dans MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client['tayara']
    collection = db['immo_neuf']

    # Effacer la collection avant de réinsérer les données (optionnel)
    collection.delete_many({})

    # Insertion des données du fichier JSON dans MongoDB
    with open('tayara_immo_neuf_raw.json', 'r', encoding='utf-8') as f:
        loaded_listings = json.load(f)
        collection.insert_many(loaded_listings)

    # Préparation des données pour le CSV
    csv_data =[]
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
            'image_urls': listing['images'],
            'listing_type': 'normal'
        })

    if golden_listing:
        csv_data.append({
            'id': golden_listing['id'],
            'title': golden_listing['title'],
            'description': golden_listing['description'],
            'price': golden_listing.get('price', ''),
            'phone': golden_listing.get('phone', ''),
            'delegation': golden_listing['location']['delegation'],
            'governorate': golden_listing['location']['governorate'],
            'publishedOn': golden_listing['metadata']['publishedOn'],
            'isModified': golden_listing['metadata'].get('isModified'),
            'state': golden_listing['metadata']['state'],
            'subCategory': golden_listing['metadata']['subCategory'],
            'isFeatured': golden_listing['metadata']['isFeatured'],
            'publisher_name': golden_listing['metadata']['publisher']['name'],
            'publisher_isShop': golden_listing['metadata']['publisher']['isShop'],
            'producttype': golden_listing['metadata'].get('producttype'),
            'image_urls': golden_listing['images'],
            'listing_type': 'golden'
        })

    df = pd.DataFrame(csv_data)
    df.to_csv('tayara_immo_neuf_structured.csv', index=False, encoding='utf-8-sig')

    # Calcul des statistiques
    normal_listings_count = df['listing_type'].value_counts().get('normal', 0)
    golden_listings_count = df['listing_type'].value_counts().get('golden', 0)
    total_listings = len(df)

    # Affichage des statistiques
    print("Data has been saved to tayara_immo_neuf_structured.csv")
    print("Data has been saved to tayara_immo_neuf_raw.json")
    print("Data has been inserted into MongoDB from JSON file")
    print("\n--- Listing Statistics ---")
    print("Normal Listings:", normal_listings_count)
    print("Golden Listings:", golden_listings_count)
    print("Total Listings:", total_listings)
    print("-------------------------")

except Exception as e:
    print("Error occurred:", e)

finally:
        conn.close()

print(data.decode("utf-8"))