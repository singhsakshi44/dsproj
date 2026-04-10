import json

from util.logger import log_message


def generate_summary_file(config_info,summary_data):
    summaryfilename = config_info["paths"]["output_db_file"]

    log_message(f"Summary file generating {summaryfilename}")

    with open(summaryfilename, "w") as f:
        json.dump(summary_data, f, indent=4)

    log_message("Summary file done")

def generate_output_file(config_info,output_data): 
    outputfilename = config_info["paths"]["output_table_file"]  

    log_message(f"Output file generating {outputfilename}")

    # print dict to pretty json
    with open(outputfilename, "w") as f:
        json.dump(output_data, f, indent=4) 

    log_message("Output file done")
