#!pip install pymongo

# importing module
from pymongo import MongoClient

hostname = "mo5pwf.h.filess.io"
database = "olistDataNoSQL_completely"
port = "27018"
username = "olistDataNoSQL_completely"
password = "dc6c7c17e95e66f637266ac829fba67e86d781e6"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]



import pandas as pd

# Read the CSV file
df = pd.read_csv('/content/product_category_name_translation.csv')

# Convert the DataFrame to a list of dictionaries
data = df.to_dict('records')

# Create or get the collection
mycollection = mydatabase["product_category_translation"]

# Insert the data into the collection
if data:
    mycollection.insert_many(data)
    print("Data uploaded successfully!")
else:
    print("No data to upload.")



# Close the connection
client.close()
print("MongoDB connection closed.")