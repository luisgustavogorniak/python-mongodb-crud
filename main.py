from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient

load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://tutorialdb:{password}@cluster0.sh9ksjd.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(connection_string)

dbs = client.list_database_names()
test_db = client.test
collections = test_db.list_collection_names()
print(collections)

# Inserting documents

def insert_test_doc():
    collection = test_db.test
    test_document = {"name": "Zé", "type": "Test"}
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id)


production = client.production
person_collection = production.person_collection


def create_documents():
    first_names = ["Moriah", "Bulat", "Gamze", "Direnç", "Kwaku", "Étaín", "Gunhild"]
    last_names = ["Kyro", "Tali", "Aeschylus", "Marius", "Kronos", "Macy", "Kalpana"]
    ages = [21, 40, 23, 19, 34, 67, 80]

    docs = []

    for first_name, last_name, age in zip(first_names, last_names, ages):
        doc = {"first_name": first_name, "last_name": last_name, "age": age}
        docs.append(doc)
        # person_collection.insert_one(doc)

    person_collection.insert_many(docs)

# Reading Documents

printer = pprint.PrettyPrinter()

def find_all_people():
    people = person_collection.find()

    for person in people:
        printer.pprint(person)


def find_kwaku():
    kwaku = person_collection.find_one({"first_name": "Kwaku"})
    printer.pprint(kwaku)


def count_all_people():
    count = person_collection.count_documents(filter={})
    print(f"Number of people: {count}")


def get_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)


def get_age_range(min_age, max_age):
    query = {"$and": [{"age": {"$gte": min_age}}, {"age": {"$lte": max_age}}]}

    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)


def project_columns():
  columns = {"_id": 0, "first_name": 1, "last_name": 1}
  people = person_collection.find({}, columns)
  for person in people:
    printer.pprint(person)

def update_person_by_id(person_id):
  from bson.objectid import ObjectId

  _id = ObjectId(person_id)

  # all_updates = {
  #   "$set": {"new_field": True},
  #   "$inc": {"age": 1},
  #   "$rename": {"first_name": "first", "last_name": "last"}
  # }
  # person_collection.update_one({"_id": _id}, all_updates)

  person_collection.update_one({"_id": _id}, {"$unset": {"new_field": ""}})

def replace_one(person_id):
  from bson.objectid import ObjectId

  _id = ObjectId(person_id) 

  new_doc = {
    "first_name": "new first name",
    "last_name": "new last name",
    "age": 100
  }

  person_collection.replace_one({"_id": _id}, new_doc)

# Deleting documents

def delete_doc_by_id(person_id):
  from bson.objectid import ObjectId
  _id = ObjectId(person_id)
  person_collection.delete_many({})

# delete_doc_by_id("63864ef0e54d25e705b928fb")  

# Relationships

address = {
  "_id": "63867cdaafec127d1e02378a",
  "street": "1363 Cambridge Place",
  "number": "2",
  "city": "Aberdeen",
  "country": "United States",
  "zip": "21001",
  "owner_id": "63867cdaafec127d"
}

person = {
  "_id": "63867cdaafec127d",
  "first_name": "John"
}

def add_address_embed(person_id, address):
  from bson.objectid import ObjectId
  _id = ObjectId(person_id)

  person_collection.update_one({"_id": _id}, {"$addToSet": {"addresses": address}})

def add_address_relationship(person_id, address):
  from bson.objectid import ObjectId
  _id = ObjectId(person_id)

  address = address.copy()
  address["owner_id"] = person_id
  
  address_collection = production.address
  address_collection.insert_one(address)

add_address_relationship("63867ed84569e88d85d32373", address)