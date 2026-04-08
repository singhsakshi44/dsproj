import json


def generate_summary_file(config_info,summary_data):
    summaryfilename = config_info["paths"]["output_dbsummary"]

    with open(summaryfilename, "w") as f:
        json.dump(summary_data, f, indent=4)

def generate_output_file(config_info,output_data): 
    outputfilename = config_info["paths"]["output_table"]  

    # print dict to pretty json
    with open(outputfilename, "w") as f:
        json.dump(output_data, f, indent=4) 
