#! /usr/local/anaconda3/envs/python_3_6/bin/python

import xmltodict
import xml.etree.ElementTree as ET
import pprint
import sys
import pymongo
# import pymongo[srv]
from pymongo.errors import BulkWriteError
import datetime
# import dnspython


def main(argv):
    # client = pymongo.MongoClient("mongodb://localhost:27017/BA?retryWrites=true")
    with open("credentials.txt", "r") as f:
        connection_string = f.read()

    client = pymongo.MongoClient(connection_string)
    db = client.xml_demo
    collection = db.xml_to_mdb


    def xml_file_to_dict(xml_source_file_name, xpath):
        tree = ET.parse(xml_source_file_name)
        nodes = tree.findall(xpath)
        docs = []
        for node in nodes:
            xml_str = ET.tostring(node, encoding='utf-8', method='xml')
            doc = xmltodict.parse(xml_str)
            doc["source_file"] = xml_source_file_name
            doc["import_date"] = datetime.datetime.now()
            docs.append(doc)
        return docs


    def load(destination_collection, inserts_list):
        try:
            destination_collection.insert_many(inserts_list, ordered=True)
        except BulkWriteError as bwe:
            pprint.pprint(bwe.details)


    docs = xml_file_to_dict("sampleData.xml", "./")
    load(collection, docs)

    docs = xml_file_to_dict("Sample-XML-With-Multiple-Records.xml", "./")
    load(collection, docs)



if __name__ == "__main__":
	main(sys.argv[1:])

# Example Queries
# db.xml_to_mdb.find({"book.genre": "Computer"}).pretty()
# db.xml_to_mdb.find({"PLANT.LIGHT": "Sunny"}).pretty()




# Make key be file and element identifier
# def xml_file_to_dict(xml_source_file_name, xpath):
#     tree = ET.parse(xml_source_file_name)
#     nodes = tree.findall(xpath)
#     docs = []
#     for node in nodes:
#         xml_str = ET.tostring(node, encoding='utf-8', method='xml')
#         doc = xmltodict.parse(xml_str)
#         [[key, value]] = doc.items()
#         key_string = xml_source_file_name.replace(".", "_") + "|" + key
#         doc[key_string] = doc.pop(key)
#         docs.append(doc)
#         print(json.dumps(doc, indent=4))
#     return docs


# def query_xml(xml_source, xpath):
    #     tree = ET.parse(xml_source)
    #     leafs = tree.findall(xpath)
    #     return leafs
    #
    #
    # def xml_elements_to_dict(xml_source):
    #     items = xml_source
    #     docs = []
    #     for item in items:
    #         xmlstr = ET.tostring(item, encoding='utf-8', method='xml')
    #         doc = xmltodict.parse(xmlstr)
    #         # [[key, value]] = doc.items()
    #         # print(key)
    #         # print(value)
    #         docs.append(doc)
    #         print(json.dumps(doc, indent=4))
    #     return docs #, key



    # xml_docs = query_xml("sampleData.xml", "./")
    # docs = xml_elements_to_dict(xml_docs)
    # load(collection, docs)
    #
    # xml_docs = query_xml("Sample-XML-With-Multiple-Records.xml", "./")
    # docs = xml_elements_to_dict(xml_docs)
    # load(collection, docs)


# books = query_xml("sampleData.xml", ".//book")
# xml_elements_to_dict(books, "book")
#
# plants = query_xml("Sample-XML-With-Multiple-Records.xml", ".//PLANT")
# books_from_catalog = query_xml("Sample-XML-With-Multiple-Records.xml", ".//BOOK")
# xml_elements_to_dict(plants, "PLANT")
# xml_elements_to_dict(books_from_catalog, "BOOK")



# xml_data = tree.getroot()
# books = xml_data.findall("./book")
# print(books)
# # e = ET.ElementTree(ET.fromstring(xml_string))
# for book in books:
#      print("xmlString: ", ET.tostring(book), "\n")
#      xmlstr = ET.tostring(book, encoding='utf-8', method='xml')
#      # print(xmlstr)
#      doc = xmltodict.parse(xmlstr)
#      print(doc)
#      # for e in doc.iter():
#      #    print("dict: ", e, "\n")
#
#     pp = pprint.PrettyPrinter(indent=4)
#     pp.pprint(json.dumps(doc))
#     pprint.pprint(doc)

# pp = pprint.PrettyPrinter(indent=4)
# pp.pprint(json.dumps(doc))
# pprint.pprint(doc)
# doc = xmltodict.parse(book)
# pprint.pprint(doc)

# pp = pprint.PrettyPrinter(indent=4)
# pp.pprint(json.dumps(doc))
# collection.insert()

# path = "sampleData.xml"
# data = json.load(path)

# root = tree.getroot()
        # print(root)