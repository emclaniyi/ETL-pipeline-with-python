import os
from zipfile import ZipFile

"""filepath = "C:\\Users\\User\\Downloads\\spotify_dataset.zip"


with ZipFile(filepath, mode='r') as f:
    name_list = f.namelist()
    print('List of files:', name_list)
    extract_path = f.extractall(path="C:\\Users\\User\\PycharmProjects\\ETL-pipeline-with-python")
    print('Extract Path:', extract_path)"""


#paths

base_path = "C:\\Users\\User\\PycharmProjects\\ETL-pipeline-with-python"
source_url = "C:\\Users\\User\\Downloads\\spotify_dataset.zip"
source_path = f"{base_path}/data/raw"


def create_directory(path):
    """
    Creates directory to store downloaded zip file and checks if folder already exists
    :param path:
    :return:
    """
    os.makedirs(os.path.dirname(path), exist_ok=False)


def extract_csv(source_url, source_path):
    #create_directory(source_path)
    with ZipFile(source_url, mode='r') as f:
        name_list = f.namelist()
        print('List of files:', name_list)
        extract_path = f.extractall(path=source_path)
        print('Extract Path:', extract_path)


extract_csv(source_url, source_path)