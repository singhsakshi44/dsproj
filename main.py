import os
from util.helper import *
from pipeline.extractor import process_folder
from pipeline.loader import *
from pipeline.transformer import *


base_dir = os.getcwd()   # project root where main.py is stored

config_info = load_config(base_dir)

file_data = process_folder(config_info)

# for item in file_data:
#     print(item)

output_data = convert_table_result(file_data)
summary_data = convert_db_result(config_info,file_data)

generate_output_file(config_info,output_data)

generate_summary_file(config_info,summary_data)
