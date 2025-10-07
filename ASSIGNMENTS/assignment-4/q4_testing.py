from pymongo import MongoClient
import pprint

# MongoDB Atlas Connection
client = MongoClient(
    "mongodb+srv://142201020:sanvithA@mlops.sicngy9.mongodb.net/"
)

db = client["retail_db"]
cust_col = db["customer_centric"]

# Fetch & Print

# Count total documents
count = cust_col.count_documents({})
print(f"Total customer-centric documents in cluster: {count}")

# Fetch one sample document
print("\nSample document:")
doc = cust_col.find_one({})
pprint.pprint(doc)

# Fetch first 2 customers from India (if any exist)
print("\nFirst 2 customers from India:")
for d in cust_col.find({"country": "India"}).limit(2):
    pprint.pprint(d)
