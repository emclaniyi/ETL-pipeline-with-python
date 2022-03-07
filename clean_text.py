import re
import os
import csv

text = '"Fiddler On The Roofâ€ Motion Picture Chorus'
text1 = '$NOT'
text1 = text1.lower()
text1 = re.sub("['\"\[\]()*$]", "", text)
print(text1)

base_path = "C:\\Users\\EM\\PycharmProjects\\ETL-pipeline-with-python"
raw_path = f"{base_path}/raw/data/"
print(os.listdir(raw_path))
df_list = []
for file in os.listdir(raw_path):
    if file.endswith('.csv'):

        with open(os.path.join(raw_path, file), mode='r', encoding='utf8') as csv_file:
            print(csv_file)
            reader = csv.DictReader(csv_file)
            track, artist = [], []
            for row in reader:
                track.append(row)
            #print(track)


def transform_new_data(raw_path, table):
    """
    Apply all transformations for each row in the .csv file before saving it into database
    """
    with open(raw_path, mode="r", encoding="windows-1252") as csv_file:
        # Read the new CSV snapshot ready to be processed
        reader = csv.DictReader(csv_file)
        # Initialize an empty list for our PprRawAll objects
        ppr_raw_objects = []
        for row in reader:
            # Apply transformations and save as PprRawAll object
            ppr_raw_objects.append(
                PprRawAll(
                    date_of_sale=update_date_of_sale(row["date_of_sale"]),
                    address=transform_case(row["address"]),
                    postal_code=transform_case(row["postal_code"]),
                    county=transform_case(row["county"]),
                    price=update_price(row["price"]),
                    description=update_description(row["description"]),
                )
            )
        # Save all new processed objects and commit
        session.bulk_save_objects(ppr_raw_objects)
        session.commit()

