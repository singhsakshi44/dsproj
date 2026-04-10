import os

from util.helper import load_config
from util.logger import initialise_logging, log_message

from util.plotchart import show_bar_chart
from pipeline.extractor import process_folder
from pipeline.loader import generate_output_file,generate_summary_file
from pipeline.transformer import convert_table_result,convert_db_result

# Main program to run the functionality

base_dir = os.getcwd()   # project root where main.py is stored

config_info = load_config(base_dir)

initialise_logging(config_info)

log_message("Process start")

file_data = process_folder(config_info)

output_data = convert_table_result(file_data)
summary_data = convert_db_result(config_info,file_data)


generate_output_file(config_info,output_data)

generate_summary_file(config_info,summary_data)

show_bar_chart(config_info,file_data)

log_message("Process complete")
