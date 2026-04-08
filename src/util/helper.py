import json
import os

def load_config(base_dir,path="config/config.json",srcdir="src"):
    config = {}
    full_path = os.path.join(base_dir, srcdir, path)
    with open(full_path, "r") as f:
        config = json.load(f)
    return config


def generate_column_mapping(col_mapping):
    revcolumn_mapping = {}

    for standard, variations in col_mapping.items():
        for col in variations:
            revcolumn_mapping[col.lower().strip()] = standard

    return revcolumn_mapping

def check_folder_hierarchy(folder_path,given_folder):
    
    parent = os.path.basename(os.path.dirname(folder_path))
    
    grandparent = os.path.basename(os.path.dirname(os.path.dirname(folder_path)))

    return parent.lower() == given_folder.lower(), grandparent