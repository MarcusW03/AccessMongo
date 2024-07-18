import numpy as np
from pymongo import MongoClient
from pymongo.errors import *
from PIL import Image, ImageChops
import time
import gridfs
import io
import pickle
import argparse

class AccessMongo:
    def __init__(self, database: str, collection: str, host: str = "localhost", port: int = 27017):
        
        self.client = MongoClient(host) #FIX PORT ERROR
        try:
            self.client.admin.command('ping')
        except ConnectionFailure:
            raise ValueError("ERROR: failed to connect to MongoDB Server -> " + host + port)
        
        self.db = self.client[database]
        self.collection = self.db[collection]
        #ONLY NEED FS IF STORING AS DATA
        self.fs = gridfs.GridFS(database=self.db, collection=collection)

    def get_from_mongo_type_data(self, id: str, test: bool=False, file: str=None):
        stime = time.time()
        document = self.collection.find_one({"id": str(id)})
        if document is None:
            print("Error: Failed to find document with id", id)
            return document, document
        image_id = document.get('image_id')
        if image_id is None:
            print("Error: Document did not have key 'image_id'")
            print("Please make sure you're only calling get_from_mongo_type_data if db is storing data")
            return image_id, image_id
        feature_id = document.get('feature_id')
        if feature_id is None:
            print("Error: Document did not have key 'feature_id'")
            print("Please make sure you're only calling get_from_mongo_type_data if db is storing data")
            return feature_id, feature_id
        image_data = self.fs.get_last_version(_id=image_id)
        feature_data = self.fs.get_last_version(_id=feature_id)
        etime = time.time()
        image = Image.open(io.BytesIO(image_data.read()))
        feature = pickle.loads(feature_data.read())
        
        ftime = time.time()
        if (test and file is not None):
            file = open(file, "a")
            file.write(f'Getting data from Mongo took {etime - stime} seconds\n')
            file.write(f'Reading image and feature from extracted data took {ftime - etime} seconds\n')
            file.write(f'Total elapsed time was {ftime - stime} seconds\n')
            file.close()
        return image, feature
    
    def get_from_mongo_type_paths(self, id: str, test: bool=False, file: str=None):
        stime = time.time()
        document = self.collection.find_one({"id": id})
        if document is None:
            #print("Error: Failed to find document with id", id)
            return document, document
        image_path = document.get('image')
        if image_path is None:
            print("Error: Document did not have key 'image'")
            print("Please make sure you're only calling get_from_mongo_type_paths if db is storing paths")
            return image_path, image_path
        feature_path = document.get('feature')
        if feature_path is None:
            print("Error: Document did not have key 'image'")
            print("Please make sure you're only calling get_from_mongo_type_paths if db is storing paths")
            return feature_path, feature_path
        etime = time.time()
        image = Image.open(image_path)
        feature = np.load(feature_path)

        ftime = time.time()
        if (test and file is not None):
            file = open(file, "a")
            file.write(f'Getting data from Mongo took {etime - stime} seconds\n')
            file.write(f'Reading image and feature from extracted paths took {ftime - etime} seconds\n')
            file.write(f'Total elapsed time was {ftime - stime} seconds\n')
            file.close()
        return image, feature

    def insert_one_paths(self, id: str, image_path: str, feature_path: str):
        document = {
            "id": id, 
            "image": image_path, 
            "feature": feature_path
        }
        result = self.collection.insert_one(document)
        return result.inserted_id
    
    def insert_one_data(self, id: str, image_path: str, feature_path: str):
        
        with open(image_path, 'rb') as image_file:
            image_id = self.fs.put(image_file, id=id)
        
        feature_bytes = np.load(feature_path, allow_pickle=True)
        feature_bytes = pickle.dumps(feature_bytes)
        feature_id = self.fs.put(feature_bytes, id=id)

        document = {
            "id": id, 
            "image_id": image_id, 
            "feature_id": feature_id, 
        }

        result = self.collection.insert_one(document)
        return result.inserted_id

    def insert_many_data(self, list_of_documents: list):

        if (len(list_of_documents) == 0):
            print("Cannot insert from list of size 0")
            return False
        
        for i in range(len(list_of_documents)):
            
            document = list_of_documents[i]
            id = document.get("id")
            image_path = document.get("image")
            feature_path = document.get("feature")
            if ((id is None) or (image_path is None) or (feature_path is None)):
                print("Inserting Failed")
                print("Please provide a list of dictionaries formatted as:")
                print("\t ['{''id': IMAGE_ID, 'image': IMAGE_PATH, 'feature': FEATURE_PATH}, ...]")
                return False

            self.insert_one_data(id, image_path, feature_path)
        
        return True
            
    def insert_many_paths(self, list_of_documents: list):

        if (len(list_of_documents) == 0):
            print("Cannot insert from list of size 0")
            return False
        
        for i in range(len(list_of_documents)):
            
            document = list_of_documents[i]
            id = document.get("id")
            image_path = document.get("image")
            feature_path = document.get("feature")
            if ((id is None) or (image_path is None) or (feature_path is None)):
                print("Inserting Failed")
                print("Please provide a list of dictionaries formatted as:")
                print("\t ['{''id': IMAGE_ID, 'image': IMAGE_PATH, 'feature': FEATURE_PATH}, ...]")
                return False           
            
            self.insert_one_paths(id, image_path, feature_path)

        return True 
    
    def delete_paths_single(self, id: str):
        #Using many to ensure total erasure of document
        #However, since id should be unique for each document, .find_one_and_delete will also work
        self.collection.delete_many({"id": id})

    def delete_paths_many(self, list_of_ids: list[str]):
        for i in range(len(list_of_ids)):
            self.delete_paths_single(list_of_ids[i])
    
    def delete_data_single(self, id: str):

        document = self.collection.find_one({"id": id})
        image_id = document.get('image_id')
        feature_id = document.get('feature_id')
        self.fs.delete(image_id)
        self.fs.delete(feature_id)
        self.collection.delete_many({"id": id})

    def delete_data_many(self, list_of_ids: list[str]):
        for i in range(len(list_of_ids)):
            self.delete_data_single(list_of_ids[i])

def main():
    
    parser = argparse.ArgumentParser(description='Mongo Uploader')
    parser.add_argument('--test', type=str, default='false')
    parser.add_argument('--file', type=str, default='test_results.txt')
    parser.add_argument('--num_tests', type=int, default=100)
    parser.add_argument('--upload', type=str, default='false')
    ##Come back to deal with starting index
    parser.add_argument('--data', type=str, default='false')
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=21017)
    parser.add_argument('--database', type=str, default='image-visualizations')
    parser.add_argument('--collection', type=str, required=True)
    args = parser.parse_args()

    try:
        mongo = AccessMongo(args.database, args.collection, args.host, int(args.port))
    except ValueError:
        print("Failed to Connect to Mongo Server")
        return
    print("Initialized Mongo Connection")
    
    if (args.upload == 'true'):
        if (args.data == 'true'):
            print("Uploading to images-data for testing")
            insert_many_for_tests(mongo, 6, data=True)
        else:
            print("Uploading to images-paths for testing")
            insert_many_for_tests(mongo, 6, data=False)
        return
    
    if (args.test == 'true'):
        if (args.data == 'true'):
            run_test(mongo, args.file, num_images=6, num_tests=args.num_tests, data=True)
        else:
            run_test(mongo, args.file, num_images=6, num_tests=args.num_tests, data=False)
        return

    #ADD OTHER FUNCTIONALITY HERE
    '''
    id = '1000' #Random for now
    image_path = '/Users/marcusw/Downloads/HW Folders/Summer 24/Learning/test_mongo/tester_image.jpg'
    feature_path = '/Users/marcusw/Downloads/HW Folders/Summer 24/Learning/test_mongo/tester_array.npy'

    #delete(collection, id)

    #insert_one_paths(collection, id, image_path, feature_path)
    #image, feature = get_from_mongo_paths(collection, id)

    #insert_one_data(collection, id, image_path, feature_path)
    
    image, feature = get_from_mongo_data(collection, id)
    original_image = Image.open(image_path)
    diff = ImageChops.difference(image, original_image)
    if diff.getbbox():
        print("images are different")
    else:
        print("images are the same")
    original_feature = np.load(feature_path)
    print(np.array_equal(feature, original_feature))
    #image.save('new_image.jpg')
    np.save('new_array', feature)'''

def insert_many_for_tests(mongo, num_images, starting_id=1000, data=False):

    for i in range(num_images):
        id = str(starting_id + i)
        image_path = './test_data/image' + str(i+1) + '.jpg'
        feature_path = './test_data/image' + str(i+1) + '.npy'
        if (data):
            mongo.insert_one_data(id, image_path, feature_path)
        else:
            mongo.insert_one_paths(id, image_path, feature_path)

def run_test(mongo, filename, num_images=6, num_tests=100, data=False):
    print("RUNNING TEST")
    print("TEST RESULTS IN ", filename)
    
    starting_id = 1000
    if (data):
        print("TESTING DATA")
        for i in range(num_tests):
            for j in range(num_images):
                id = starting_id + j
                image, feature = mongo.get_from_mongo_type_data(str(id), test=True, file=filename)
                if image is None or feature is None:
                    print("TEST FAILED")
                    return
    else:
        print("TESTING PATHS")
        for i in range(num_tests):
            for j in range(num_images):
                id = starting_id + j
                image, feature = mongo.get_from_mongo_type_paths(str(id), test=True, file=filename)
                if image is None or feature is None:
                    print("TEST FAILED")
                    return
    
if __name__ == '__main__':
    main()