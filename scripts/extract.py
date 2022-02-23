import os
from zipfile import ZipFile

# paths
base_path = "C:\\Users\\User\\PycharmProjects\\ETL-pipeline-with-python"
source_url = "C:\\Users\\User\\Downloads\\spotify_dataset.zip"
raw_path = f"{base_path}/raw"


def create_directory(path):
    """
    Creates directory to store downloaded zip file and checks if folder already exists
    :param path:
    :return:
    """
    os.makedirs(os.path.dirname(path), exist_ok=False)


def extract_csv(url, path):
    # create_directory(source_path)
    with ZipFile(url, mode='r') as f:
        name_list = f.namelist()
        print('List of files:', name_list)
        f.extractall(path=path)


extract_csv(source_url, raw_path)

def main():
    print("[Extract] start")
    print("[Extract] create directory")
    print(f"[Extract] saving data to '{raw_path}'")
    print("[Extract] End")
