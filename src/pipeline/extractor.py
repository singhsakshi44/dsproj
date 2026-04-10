import os
import pandas as pd

from util.helper import *
from util.logger import log_message

from pipeline.transformer import *

def process_folder(config_info):
    file_result = {}

    log_message("Folder processing start")

    column_mapping = config_info["column_mapping"]
    folderpath = config_info["paths"]["input"]
    folder_in_scope = config_info["processing"]["scoring_folder"].strip().lower()
    agg_target_column = config_info["processing"]["target_column"]

    rev_column_mapping = generate_column_mapping(column_mapping)
    row_data = []
    start_reading = False
    parent_folder = '' 

    process_db_folder(folderpath,row_data,folder_in_scope,start_reading,parent_folder)

    file_result = read_file(row_data,rev_column_mapping,agg_target_column)

    log_message("Folder processing end")
    return file_result

def process_db_folder(folderpath:str,row_data,folder_in_scope,start_reading,parent_folder):
    result_row = {}
    current_folder = ''

    for item in os.listdir(folderpath):
        item_path = os.path.join(folderpath, item)

        log_message(f"Scanning folder {item_path}")

        # if folder go one more level down
        if os.path.isdir(item_path):
            current_folder = os.path.basename(item_path).strip()

            # if current folder is scoping then we need to start scanning nested folders and files
            if current_folder.lower() == folder_in_scope.lower():
                start_reading = True
                parent_folder = os.path.basename(os.path.dirname(item_path))
                        
            tpath_list = [part.lower() for part in os.path.normpath(item_path).split(os.sep)]

            # Once out of folder scoping then we need to stop scanning nested folders and files
            if start_reading == True and folder_in_scope.lower() not in tpath_list:
                start_reading = False 
                parent_folder = ''                

            subfolder_result = process_db_folder(item_path,row_data,folder_in_scope,start_reading,parent_folder)
            
            # sub folfder will have blank result if not valid candidate to read
            if subfolder_result:
                result_row[item] = subfolder_result

        # read csv from scoring folder only
        elif item.endswith(".csv") and start_reading == True:           
            tfile_folder = os.path.basename(os.path.dirname(item_path))
            row_data.append([parent_folder, tfile_folder, item, item_path])
            result_row[item] = 0

        log_message(f"Scanning done folder {item_path}")

    return result_row

def read_file(file_info,rev_column_mapping,agg_target_column):
    result_row = []

    for item in file_info:
        item_path = item[3]
        log_message(f"Reading File {item_path}")

        # If not valid path then go to next iteration
        if not os.path.isfile(item_path):
            continue   

        tdf = pd.read_csv(item_path)
        tdf = standardrize_columns(tdf,rev_column_mapping)

        if agg_target_column.lower() in tdf.columns.str.lower():
            try:
                tdf[agg_target_column] = pd.to_numeric(tdf[agg_target_column], errors="raise")
                score = tdf["finalscore"].mean()
                score = 0 if pd.isna(score) else float(score)    
            except Exception as e:
                log_message(f"File {item_path} Conversion error {e}","error")                
        else:
            score = 0
        result_row.append([item[0], item[1], item[2],round(score, 4)])

        log_message(f"End File {item_path}")

    return result_row