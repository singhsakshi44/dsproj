import json
import os

def load_config(base_dir,path="config/config.json",srcdir="src"):
    config = {}
    full_path = os.path.join(base_dir, srcdir, path)
    with open(full_path, "r") as f:
        config = json.load(f)
    return config

def normalise_column(col_name):
    new_col_name = col_name.lower().strip().replace('_', '').replace(' ', '')
    return new_col_name


def generate_column_mapping(col_mapping):
    revcolumn_mapping = {}

    for standard, variations in col_mapping.items():
        for col in variations:
            revcolumn_mapping[normalise_column(col)] = standard

    return revcolumn_mapping
