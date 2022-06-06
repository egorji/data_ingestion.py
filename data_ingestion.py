
import os
import pprint
import json
import shutil
from queue import Queue
import pymongo
import pickle
from datetime import date
import fitz
from pdf2image import convert_from_path
# import easyocr
import numpy as np
import pdf2image
import cv2
import pytesseract
import spacy
import math
# from PyPDF2 import PdfFileMerger, PdfFileReader
import shutil


path=r"C:\Users\Efat\Desktop\seneca\MSH_Trinetra project\classified_data"

class Ingestion():

    def __init__(self):

        self.category_repo={1:"Collective Agreements",2:"Benefits"}   ##will be recieved from API

        # try:
        #     with open("documents_json_file.json","r") as json_file:
        #         self.json_file=json.load(json_file)
        # except:
        #     self.json_file=[]

        self.action_queue=Queue(maxsize=40)

        self.num_sent_per_snippet=5
        self.connectionString="mongodb://localhost:27017/"
        self.db, self.pagesCollection, self.snippetsCollection =self.initialize_mongodb_collections()
        self.tesseract_cmd=r"C:\Program Files\Tesseract-OCR\tesseract.exe"
        self.tessdata_dir_config=r"C:\Program Files\Tesseract-OCR\tessdata"
        self.poppler_path=r"C:\Program Files\poppler-0.68.0\bin"

    def check_action_queue(self):   ##action queue should also contain category addition and removal
        message=''
        while self.action_queue.empty()==False:
            for item in list(self.action_queue.queue):

                if item['action_code']==0:    #action_cide=0 represents removing document
                    message += self.remove_one_doc(item['document_name'])+"\n"
                    #self.action_queue.get(item)

                elif item['action_code']==1:   #action_cide=1 represents uploading document
                    message += self.upload_one_doc(item['document_id'], item['category_id'],
                                                  item['document_name'],
                                                  item['file_link'], item['file_path'],
                                                  item['document_tags'],item['document_expiryDate'])+"\n"

                elif item['action_code']==2:
                    message += self.add_main_category(item['category_name'],item['category_id'])+"\n"

                elif item['action_code']==3:
                    message += self.omit_category(item['category_id'])+"\n"

                else:
                    self.action_queue.get(item)
                    message += "Required action not valid. Action code: {}".format(item['action_code'])+"\n"

                self.action_queue.get(item)
        return message


    def recieve_document_to_upload(self,document_id, category_id, document_name, file_link, file_path, document_tags, document_expiryDate):
        """ gets the document info from the API, and adds the document upload action to the system action queue.
        the imported expiry date must be come in iso format ('yyyy-mm-dd')"""
        document={}
        document['document_id']= document_id
        document['category_id'] = category_id
        document['document_name']=document_name
        document['file_link']=file_link
        document['file_path']= file_path
        document['document_tags'] = document_tags
        document['document_expiryDate']= document_expiryDate
        document['action_code']=1
        self.action_queue.put(document)

    def recieve_document_to_remove(self, document_name):
        document={}
        document['document_name']=document_name
        document['action_code']= 0
        self.action_queue.put(document)

    def recieve_category_to_add(self, category_name, category_id):
        category={}
        category['category_name']=category_name
        category['category_id']= category_id
        category['action_code']=2
        self.action_queue.put(category)


    def recieve_category_to_remove(self, category_id):
        category={}
        category['category_id']=category_id
        category['action_code']=3
        self.action_queue.put(category)


    def load_file(self, path, file_link):
        """ reads a pdf file page by page using pdfplumber
        along with its metadata given the path to the file,
        and returns a dictionary with page numbers as keys and page texts as values
        as well as a dictionary of the document metadata"""
        whole_text=''
        doc = fitz.open(path)
        metadata=doc.metadata
        pages=[]
        page_num = 1
        for page in doc:
            page_dict = {}
            text = page.get_text()
            snippets=self.generate_snippets(text,self.num_sent_per_snippet)   #number 5 to be experimented and modified
            whole_text+=text
            page_dict["page_number"] = page_num
            page_dict["page_text"] = text
            page_dict["page_snippets"]=snippets    #create snippets directly out of the page text
            page_dict["page_link"]=file_link+"#page="+str(page_num)
            pages.append(page_dict)
            page_num += 1
        if whole_text=="":    #the loaded pdf document is unsearchable
            pages=self.extract_text_from_scanned_pdf(path,file_link)
        return pages , metadata
        #pages=[{"page_number":1, "page_text":"..."},{"page_number":2, "page_text':"..."},...]

    def extract_text_from_scanned_pdf(self,path,file_link):
        """using pytesseract, converts a unsearchable/scanned pdf files to searchable one\
        , extracts the text out of it, and returns the dictionary of page numbers and page texts.
         There is potential possibility to save the converted file to a defined directory or
         even to replace the scanned files with their searchable copy in the same local directory."""

        pytesseract.pytesseract.tesseract_cmd = self.tesseract_cmd
        #no need for this command here as tesseract has been installed locally and\
        # its variable has been defined in the local system variables
        # TESSDATA_PREFIX = r"C:\Program Files\Tesseract-OCR"
        tessdata_dir_config = self.tessdata_dir_config

        images = pdf2image.convert_from_path(pdf_path=path, fmt="jpg",
                                             poppler_path=self.poppler_path)

        pages=[]
        page_num = 1
        for image in images:
            image.save('image_converted.jpg')
            img = cv2.imread('image_converted.jpg', 1)

            result=pytesseract.image_to_string(img, lang="eng", config=tessdata_dir_config)
            page_dict={}
            page_dict['page_number']=page_num
            page_dict['page_text'] = result
            snippets=self.generate_snippets(result,self.num_sent_per_snippet)
            page_dict['page_snippets']=snippets
            page_dict['page_link'] =file_link+"#page="+str(page_num)
            page_num+=1

            pages.append(page_dict)
        shutil.rmtree('image_converted.jpg')
        return pages

    def generate_snippets(self, text, num_sent):
        sent_tokenizer_model = spacy.load("en_core_web_sm")
        sentences = []
        doc = sent_tokenizer_model(text)
        # create a list of sentences out of the page text
        for sentence in doc.sents:
            sentences.append(str(sentence))
        snippets = []
        i = 0
        last_index = 0
        snippet_num = 1
        while i < len(sentences):
            snippet = {}
            snippet["snippet_id"] = snippet_num
            snippet["snippet_text"] = ' '.join(sentences[i:i + num_sent])

            snippets.append(snippet)

            last_index = i + num_sent

            i += int(math.ceil(num_sent / 2))
            snippet_num += 1

        if last_index < len(sentences):
            snippet = {}
            snippet["snippet_id"] = snippet_num
            snippet["snippet_text"] = ' '.join(sentences[last_index:])

            snippets.append(snippet)

        return snippets


    def create_snippets_list_from_document(self, document_dict):
        """ given a document record from the document pages collection, it creates a list of \
        snippet objects out of all the document pages. Each snippet object is a dictionary containing
        all the needed information from the document and the page the snippet has been created from."""

        snippets=[]
        for page in document_dict['pages']:
            for snippet in page['page_snippets']:
                snippet_dict={}
                snippet_dict['document_id']=document_dict['document_id']
                snippet_dict['document_name']=document_dict['document_name']
                snippet_dict['category_id']=document_dict['category_id']
                snippet_dict['category_name']=document_dict['category_name']
                snippet_dict['document_tags']=document_dict['document_tags']
                snippet_dict['page_number']=page['page_number']
                snippet_dict['page_link']=page['page_link']
                snippet_dict['snippet_id']=snippet['snippet_id']
                snippet_dict['snippet_text']=snippet['snippet_text']

                snippets.append(snippet_dict)

        return snippets


    def upload_one_doc(self, document_id, category_id, document_name, file_link, file_path, document_tags, document_expiryDate):
        """ given a path to a new document, it calls document_exist() check if the document exist in the database
        in terms of the document metadata and document name.
        if the document already exist, it prevents duplication by providing the user with a notice.
        if the document is new to the dataset, it calls load_file() to read the file page by page and updates
        the database collection along with the FAISS database accordingly."""
        if self.document_exist(document_name):
            return "Uploading document {} failed since the document already exist in the dataset.".format(document_name)
        else:      #create the document object to be imported to the dataset
            document_dict={}

            try:
                pages, metadata=self.load_file(file_path, file_link)
            except:
                return "Uploading document {} failed since file could not be loaded from the given path.".format(document_name)



            document_dict["document_id"] = document_id
            document_dict["document_name"] = document_name
            document_dict["category_id"] = category_id
            document_dict["category_name"] = self.category_repo[category_id]
            document_dict["document_link"] = file_link
            document_dict["document_tags"] = document_tags
            document_dict["document_expiryDate"] = document_expiryDate
            document_dict["document_metadata"] = metadata
            document_dict["pages"]=pages
            document_dict["document_status"] = "Active"  # all the primary loaded documents are active
            document_dict["date_uploaded"]=date.today().isoformat()


            #append the document object to the system json file
            # for item in self.json_file:
            #     if "_id" in item.keys():
            #         item.pop("_id")
            # self.json_file.append(document_dict)
            #
            # with open("documents_json_file.json","w") as write_json:
            #     json.dump(self.json_file,write_json, indent=4)

            #update the mongo databases
            try:     ###might need fixing to change the structure of try/except statements
                self.pagesCollection.insert_one(document_dict)
            except:
                return "Uploading document {} failed since the document pages\
                 have not been imported to the database successfuly.".format(document_name)

            snippets=self.create_snippets_list_from_document(document_dict)

            try:
                self.snippetsCollection.insert_many(snippets)
            except:
                self.pagesCollection.delete_one({'document_name':document_name})
                return "Uploading document {} failed since The document snippets\
                 have not been imported to the database successfuly.".format(document_name)

            # update FAISS database here
            # try:
            #     update FAISS database
            # except:
            #     self.pagesCollection.delete_one({'document_name': document_name})
            #     self.snippetsCollection.delete_many({'document_name': document_name})
            #     return "The vectorization process has been crushed."

            return  "Document {} was uploaded to the system successfuly.".format(document_name)


    # def upload_multiple_documents(self,category_id,path_to_folder):   #to be modified
    #     """ loops through all the pdf files under the category and call upload_one_doc method to read them and
    #     update the system documents list, and mongo database accordingly."""
    #
    #     for file in os.listdir(path_to_folder):       #loop through the files under the category
    #         file_full_path = os.path.join(path_to_folder, file)
    #         self.upload_one_doc( category_id, file_full_path)

    def document_exist(self, document_name):   #to be fixed, self.documents wont exist anymore
        """ checks whether or not the document intended to be uploaded to the dataset
        under a specific category, already exist."""

        documents= self.retrieve_data_from_mongodb(self.pagesCollection, {})

        exist=False
        for doc in documents:
            if doc['document_name']==document_name:
                exist=True
        if exist== True:
            return True
        else:
            return False


    def initialize_mongodb_collections(self):
        """ creates a mongodb database and laods the primary data json file to it.
        In the created mongodb collection, each page represents a document."""

        projectClient = pymongo.MongoClient(self.connectionString)
        db = projectClient.HRESdataset
        pagesCollection = db.pdfDocuments
        snippetsCollection = db.snippets
        return db, pagesCollection, snippetsCollection

    def retrieve_data_from_mongodb(self, collection, query):
        """given a query, it retrieves documents matched with the query from the mongo database"""
        data=collection.find(query)
        doc_list=[]
        for doc in data:
            doc_list.append(doc)
        return doc_list

    def add_main_category(self,category_name, category_id):
        """adds category to the existing list of categories"""
        if category_name in self.category_repo.values():
            return "Category {} already exist in the database.".format(category_name)
        else:
            self.category_repo[category_id]=category_name
            return "Category {} was added to the database successfuly.".format(category_name)

    def omit_category(self,category_id):
        """removes category from the list of categories along with all the documents under the category"""

        if category_id in self.category_repo.keys():
            category_name=self.category_repo[category_id]
            self.category_repo.pop(category_id)
            try:
                query={"category_id": category_id}
                documents_to_remove=self.retrieve_data_from_mongodb(self.pagesCollection,query)
                self.pagesCollection.delete_many(query)
            except:
                return "Removing category {} failed since deleting the documents under it was not successful.".format(category_name)
            try:
                self.snippetsCollection.delete_many({"category_id":category_id})
            except:
                self.pagesCollection.insert_many(documents_to_remove)
                return "Removing category {} failed since deleting the snippets under it was not successful.".format(category_name)
            return "Category {} with the documents under it were removed successfuly from the database.".format(category_name)
        else:
            return "Removing category failed since this category does not exist in the database."


        # # delete the documents under the category from the json file
        # for document in self.json_file:
        #     if "_id" in document.keys():
        #         document.pop("_id")
        #     if document["category_id"] == category_id:
        #         self.json_file.remove(document)
        # with open("documents_json_file.json", "w") as write_json:
        #     json.dump(self.json_file, write_json, indent=4)

    def remove_one_doc(self, document_name):
        """ removes a document from the database given the document name and the category id."""
        #check to see if the document exist in the dataset
        if not self.document_exist(document_name):
            return "Document {} does not exist in the dataset.".format(document_name)
        else:
            document=self.retrieve_data_from_mongodb(self.pagesCollection,{'document_name':document_name})
            #remove the document from mongodb
            try:
                self.pagesCollection.delete_one({"document_name":document_name, "category_id":category_id})
            except:
                return "Document has not been removed from the collection of documents pages."

            try:
                self.snippetsCollection.delete_many({"document_name":document_name, "category_id":category_id})
            except:
                self.pagesCollection.insert_one(document[0])
                return "Document snippets have not been removed from the collection of snippets"

            # # delete the document from the json file
            # for document in self.json_file:
            #     if "_id" in document.keys():
            #         document.pop("_id")
            #     if document["document_name"] == document_name:
            #         self.json_file.remove(document)
            #
            # with open("documents_json_file.json", "w") as write_json:
            #     json.dump(self.json_file, write_json, indent=4)


            return "Document {} removed from the database successfuly.".format(document_name)

    def remove_multiple_documents(self,category_id, listOfDocs):
        """given a list of file names and a category removes the files"""
        for document_name in listOfDocs:
            self.recieve_document_to_remove(category_id, document_name)


    def change_document_status(self, document_name, status):
        """given a document name and the intended status for the document,\
        changes the document status to the given status."""
        if not self.document_exist(document_name):
            return "Document {} does not exist in the dataset."
        else:
            query={'document_name':document_name}
            document=self.retrieve_data_from_mongodb(self.pagesCollection, query)
            old_status=document[0]["document_status"]
            if old_status==status:
                return "The status of document {} is already {}.".format(document_name,status)
            else:

                #change the document status in the json file
                # for document in self.json_file:
                #     if "_id" in document.keys():
                #         document.pop("_id")
                #         if document["document_name"] == document_name:
                #                 document["document_status"]=status
                #         with open("documents_json_file.json", "w") as write_json:
                #             json.dump(self.json_file, write_json, indent=4)

                #change the document status in mongodb
                edit = {"$set": {"document_status": status}}
                try:
                    self.pagesCollection.update_one(query, edit)
                except:
                    return "Document {} status change failed due to status change failure in the collection of document pages."\
                        .format(document_name)
                try:
                    self.snippetsCollection.update_many(query, edit)
                except:
                    self.pagesCollection.update_one(query, {"$set": {"document_status": old_status}})
                    return "Document {} status change failed due to status change failure in the collection of snippets."

                return "Document {} status changed to {}.".format(document_name, status)


    def retrieve_category_documents(self,category_id):
        """ takes the category_id as the input and returns a list of documents names stored under it"""
        documents_under_category=[]
        query={'category_id':category_id}
        documents=self.retrieve_data_from_mongodb(self.pagesCollection,query)
        for doc in documents:
            documents_under_category.append(doc["document_name"])

        return documents_under_category

    def retrieve_categories(self):   #to be fixed
        """ returns a list of category names existing in the system"""
        category_list=[]
        for id in self.category_repo.keys():
            category_list.append(self.category_repo[id])
        return category_list

    def retrieve_document_info(self, document_name):
        query={'document_name':document_name}
        document=self.retrieve_data_from_mongodb(self.pagesCollection, query)
        doc_info={}
        doc_info["document_name"]=document[0]["document_name"]
        doc_info["category_name"]=document[0]["category_name"]
        doc_info["status"]=document[0]["document_status"]
        doc_info["date_uploaded"]=document[0]["date_uploaded"]
        doc_info["metadata"]=document[0]["document_metadata"]
        doc_info["expiry_Date"]=document[0]["document_expiryDate"]
        string=""
        for key in list(doc_info.keys()):
            string+=str(key)+" : "+str(doc_info[key])+"\n"
        return string

    def expire_doc_auto(self):
        message=''
        today=date.today()
        documents=self.retrieve_data_from_mongodb(self.pagesCollection,{"document_expiryDate":{"$ne":None}})
        for doc in documents:
            if date.fromisoformat(doc["document_expiryDate"])<today:
                id=doc["document_id"]
                try:
                    self.pagesCollection.update_one({"document_id":id},{"$set":{"document_status":"Expired"}})
                    try:
                        self.snippetsCollection.update_many({"document_id": id}, {"$set": {"document_status": "Expired"}})
                        message += "Document {} status has been changed to Expired.".format(doc["document_name"])+"\n"+\
                                   "expiry date: {}".format(doc["document_expiryDate"])+"\n\n"
                    except:
                        message += "Document {} expiry date has passed, but it could not be expired in the database." \
                                       .format(doc["document_name"]) + "\n" + "expiry date: {}".format(doc["document_expiryDate"]) \
                                   + "\n\n"
                        self.pagesCollection.update_one({"document_id": id}, {"$set": {"document_status": "Active"}})
                except:
                    message+= "Document {} expiry date has passed, but it could not be expired in the database."\
                    .format(doc["document_name"])+"\n"+"expiry date: {}".format(doc["document_expiryDate"])+"\n\n"

        return message

    def clear_dataset(self):
        """ deletes all the documents from the database """

        # #clear the system json file
        # self.json_file=[]
        # with open("documents_json_file.json", "w") as write_json:
        #         json.dump(self.json_file, write_json)

        #clear the mongo database
        document_pages=self.retrieve_data_from_mongodb(self.pagesCollection,{})
        try:
            self.pagesCollection.delete_many({})
            try:
                self.snippetsCollection.delete_many({})
            except:
                self.pagesCollection.insert_many(document_pages)
                return "Clearing system database failed."
        except:
            return "Clearing system database failed."


path1=r"C:\Users\Efat\Desktop\seneca\MSH_Trinetra project\classified_data\benefits\2021 MROO Retiree Benefit Summary Sheet.pdf"
path2=r"C:\Users\Efat\Desktop\seneca\MSH_Trinetra project\classified_data\Collective_Agreements\L348 Markham Stouffville Central Combined FINAL expiry 2022 (POSTED)_0.pdf"
path3=r"C:\Users\Efat\Desktop\seneca\MSH_Trinetra project\classified_data\benefits\1021_Focus.pdf"
#path to the unsearchable/scanned pdf file
path4= r"C:\Users\Efat\Desktop\seneca\MSH_Trinetra project\classified_data\Collective_Agreements\ONA Local Issues - Expiry June 7 2021 - (signed).pdf"
path_to_multi_docs=r"C:\Users\Efat\Desktop\seneca\MSH_Trinetra project\classified_data\benefits"

def main():
    ingest=Ingestion()
    print("categories: ")
    print(ingest.retrieve_categories())
    #clear dataset to have an empty system for start
    ingest.clear_dataset()

    #print(ingest.retrieve_data_from_mongodb(ingest.pagesCollection,{}))
    #upload 3 documents one by one to test the upload_one_document option
    ingest.recieve_document_to_upload(1,2,"2021 MROO Retiree Benefit Summary Sheet.pdf",'',path1,['union','retired'],None)
    ingest.recieve_document_to_upload(2,1,"L348 Markham Stouffville Central Combined FINAL expiry 2022 (POSTED)_0.pdf",'',path2,['non-union','nurses'],'2022-05-23')
    ingest.recieve_document_to_remove("2021 MROO Retiree Benefit Summary Sheet.pdf")
    ingest.recieve_category_to_add('contracts',3)
    ingest.recieve_category_to_remove(2)

    print(len(ingest.action_queue.queue))

    print(ingest.check_action_queue())
    print(ingest.category_repo)
    ingest.expire_doc_auto()
    #(ingest.retrieve_data_from_mongodb(ingest.pagesCollection, {}))
    pprint.pprint(ingest.retrieve_data_from_mongodb(ingest.snippetsCollection, {}))
    # print(ingest.upload_one_doc(2,path1))
    # print(ingest.upload_one_doc(1,path2))
    # print(ingest.upload_one_doc(2,path3))
    # print(ingest.upload_one_doc(1, path4))

    #remove one file from the system to test the remove method
    # print(ingest.remove_one_file(2,'2021 MROO Retiree Benefit Summary Sheet.pdf'))

    #upload multiple documents
    # ingest.upload_multiple_documents(2,path_to_multi_docs)

    #print the list of documetns under each category
    # print("docs under categories:")
    # print("category 1:", ingest.retrieve_category_documents(1))
    # print("category 2:", ingest.retrieve_category_documents(2))

    #print the list of all the document names with their category
    # print("documents list:")
    # for item in ingest.documents:
    #     print(item["document_name"],":",item["category_name"])

    #print a whole list of documents along with their metadata
    # print("All the documents in the dataset with their metadata:")
    # print(ingest.documents)

    #print a list of documents ids existing in the dataset
#   print("documents ids list:")
#   print(ingest.document_ids)

    #print the system json file to see its structure
    # print("json_file:")
    # print(ingest.json_file)

    #print and see the first and last item of the json file
    # print("first item of the json file:")
    # print(ingest.json_file[0])
    # print("last item of the json file:")
    # print(ingest.json_file[-1])

    #send a query to the dataset to see a specific document of the dataset
    #print(ingest.retrieve_data_from_mongodb({"document_name":"2021 MROO Retiree Benefit Summary Sheet.pdf"}))

    #for all the documents, retrieve the organized info
    # for doc in ingest.documents:
    #     print(ingest.retrieve_document_info(doc["document_name"]))
    #     print("="*15)

    #test change document status method
    # ingest.change_document_status(2,"2021 MROO Retiree Benefit Summary Sheet.pdf","Active")

    #test retrive data from mongodb
    # print(ingest.retrieve_data_from_mongodb({"category_id":2}))



if __name__=="__main__":
    main()
