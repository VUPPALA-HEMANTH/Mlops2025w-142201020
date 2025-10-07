import pandas as pd
from pymongo import MongoClient

# MongoDB Atlas Connection
client = MongoClient(
    "mongodb+srv://142201020:sanvithA@mlops.sicngy9.mongodb.net/"
)

db = client["retail_db"]
txn_col = db["transaction_centric"]
cust_col = db["customer_centric"]

# Drop existing collections (start fresh)
txn_col.drop()
cust_col.drop()

# Load CSV (first 1000 rows)
df = pd.read_csv("data/online_retail.csv")
df = df.dropna(subset=["CustomerID"])
df = df.iloc[:1000]

# Build Transaction-Centric Docs
txn_docs = []
txn_grouped = df.groupby(["InvoiceNo", "CustomerID", "InvoiceDate", "Country"])

for (invoice, customer, inv_date, country), group in txn_grouped:
    items = group[["StockCode", "Description", "Quantity", "UnitPrice"]].to_dict("records")
    total_amount = (group["Quantity"] * group["UnitPrice"]).sum()
    txn_docs.append({
        "invoice_no": str(invoice),
        "customer_id": str(int(customer)),
        "invoice_date": pd.to_datetime(inv_date).isoformat() if not pd.isna(inv_date) else None,
        "country": country,
        "total_amount": round(total_amount, 2),
        "items": items
    })

# Build Customer-Centric Docs
cust_docs = []
grouped = df.groupby(["CustomerID", "Country"])

for (customer, country), group in grouped:
    invoices = []
    total_spent = 0
    for (inv_no, inv_date), g in group.groupby(["InvoiceNo", "InvoiceDate"]):
        items = g[["StockCode", "Description", "Quantity", "UnitPrice"]].to_dict("records")
        total = (g["Quantity"] * g["UnitPrice"]).sum()
        total_spent += total
        invoices.append({
            "invoice_no": str(inv_no),
            "invoice_date": pd.to_datetime(inv_date).isoformat() if not pd.isna(inv_date) else None,
            "total_amount": round(total, 2),
            "items": items
        })
    cust_docs.append({
        "customer_id": str(int(customer)),
        "country": country,
        "total_spent": round(total_spent, 2),
        "invoices": invoices
    })

# Insert into Atlas
if txn_docs:
    txn_col.insert_many(txn_docs)
    print(f"Inserted {len(txn_docs)} transaction-centric documents into MongoDB Atlas.")
else:
    print("No transaction documents to insert.")

if cust_docs:
    cust_col.insert_many(cust_docs)
    print(f"Inserted {len(cust_docs)} customer-centric documents into MongoDB Atlas.")
else:
    print("No customer documents to insert.")
