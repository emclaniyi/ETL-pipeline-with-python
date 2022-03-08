import os
from zipfile import ZipFile
import pandas as pd

# paths
base_path = "C:\\Users\\EM\PycharmProjects\\ETL-pipeline-with-python"
source_url = "C:\\Users\\EM\\Desktop\\NEWMOVE\\spotify_dataset.zip"
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
        #drop repeated columns
        csv_file = pd.read_csv(f"{base_path}/raw/data/data.csv", low_memory=False)
        csv_file = csv_file.drop(['release_date'], axis=1)
        csv_file.to_csv(f'{raw_path}/data/tracks.csv')


def main():
    print("[Extract] start")
    print("[Extract] create directory")
    extract_csv(source_url, raw_path)
    print(f"[Extract] saving data to '{raw_path}'")
    print("[Extract] End")
