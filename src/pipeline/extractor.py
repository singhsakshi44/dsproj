import os
import pandas as pd
from pipeline.transformer import *
from util.helper import *

def process_folder(config_info):
    file_result = {}

    column_mapping = config_info["column_mapping"]
    folderpath = config_info["paths"]["input"]
    folder_in_scope = config_info["processing"]["scoring_folder"].strip().lower()

    rev_column_mapping = generate_column_mapping(column_mapping)
    row_data = []
    start_reading = False
    parent_folder = '' 

    process_db_folder(folderpath,rev_column_mapping,row_data,folder_in_scope,start_reading,parent_folder)
    # print(row_data)

    file_result = read_file(row_data,rev_column_mapping)

    return file_result

def process_db_folder(folderpath:str,rev_column_mapping,row_data,folder_in_scope,start_reading,parent_folder):
    result_row = {}
    current_folder = ''

    for item in os.listdir(folderpath):
        item_path = os.path.join(folderpath, item)

        # print(f"base path {item_path}  {os.path.basename(item_path)}")
        # print(os.path.dirname(item_path))
        # print(os.path.basename(os.path.dirname(item_path)))

        # if folder go one more level down
        if os.path.isdir(item_path):
            current_folder = os.path.basename(item_path).strip()

            # if current folder is scoping then we need to start scanning nested folders and files
            if current_folder.lower() == folder_in_scope.lower():
                # result_row = {}
                start_reading = True
                parent_folder = os.path.basename(os.path.dirname(item_path))
                        
            tpath_list = [part.lower() for part in os.path.normpath(item_path).split(os.sep)]

            # Once out of folder scoping then we need to stop scanning nested folders and files
            if start_reading == True and folder_in_scope.lower() not in tpath_list:
                start_reading = False 
                parent_folder = ''                

            subfolder_result = process_db_folder(item_path,rev_column_mapping,row_data,folder_in_scope,start_reading,parent_folder)
            
            # sub folfder will have blank result if not valid candidate to read
            if subfolder_result:
                result_row[item] = subfolder_result

        # read csv from scoring folder only
        elif item.endswith(".csv") and start_reading == True:           
            tfile_folder = os.path.basename(os.path.dirname(item_path))
            row_data.append([parent_folder, tfile_folder, item, item_path])
            result_row[item] = 0

    return result_row

def read_file(file_info,rev_column_mapping):
    result_row = []

    for item in file_info:
        item_path = item[3]
        
        # print(f"{item_path} {os.path.isfile(item_path)}")

        # If not valid path then go to next iteration
        if not os.path.isfile(item_path):
            continue   

        tdf = pd.read_csv(item_path)
        tdf = standardrize_columns(tdf,rev_column_mapping)

        if "finalscore".lower() in tdf.columns.str.lower():
            tdf["finalscore"] = pd.to_numeric(tdf["finalscore"], errors="coerce")
            score = float(tdf["finalscore"].mean())
            score = 0 if pd.isna(score) else score
            
        else:
            score = 0
        result_row.append([item[0], item[1], item[2],round(score, 4)])
    
    return result_row