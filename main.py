from pymongo import MongoClient
from pprint import pprint
import json


# Подключение к MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['alcomarket']
products = db['products']

products.drop()

with open('products.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
    products.insert_many(data)

print("\n📦 Все товары:")
for doc in products.find():
    pprint(doc)
