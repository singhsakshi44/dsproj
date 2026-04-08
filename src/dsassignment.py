
import os
import pandas as pd
import json



def generate_column_mapping(col_mapping):
    revcolumn_mapping = {}

    for standard, variations in col_mapping.items():
        for col in variations:
            revcolumn_mapping[col.lower().strip()] = standard

    return revcolumn_mapping


def standardrize_columns(tdf,revcolumn_mapping):
    new_columns = []
        
    for col in tdf.columns:
        clean_col = col.lower().strip().replace('_', '').replace(' ', '')

        new_name = ""
        if clean_col in revcolumn_mapping:
            new_name = (revcolumn_mapping[clean_col])

        if new_name != "" and new_name not in new_columns:
            new_columns.append(new_name)
        else:
            new_columns.append(clean_col)  # keep as is
        
    tdf.columns = new_columns
    return tdf

def process_folder(folderpath,rev_column_mapping):
    db_result = {}

    for item in os.listdir(folderpath):
        item_path = os.path.join(folderpath, item)
        
        if os.path.isdir(item_path):
            res = process_db_folder(item_path,rev_column_mapping)

            db_result[item] = res.get("Scoring","")

    return db_result


def process_db_folder(folderpath:str,rev_column_mapping):
    result_row = {}
    
    for item in os.listdir(folderpath):
        item_path = os.path.join(folderpath, item)

        # if folder go one more level down
        if os.path.isdir(item_path):
            subfolder_result = process_db_folder(item_path,rev_column_mapping)
            
            # sub folfder will have blank result if not valid candidate to read
            if subfolder_result:
                result_row[item] = subfolder_result

        # read csv from scoring folder only
        elif item.endswith(".csv") and "Scoring" in os.path.normpath(folderpath).split(os.sep):
            try:
                tdf = pd.read_csv(item_path)
                tdf = standardrize_columns(tdf,rev_column_mapping)

                if "finalscore".lower() in tdf.columns.str.lower():
                    tdf["finalscore"] = pd.to_numeric(tdf["finalscore"], errors="coerce")
                    score = tdf["finalscore"].mean()
                    score = 0 if pd.isna(score) else score
                    # score = float(tdf["finalscore"].mean())
                else:
                    score = 0
                result_row[item] = round(score, 4)

            except Exception as e:
                print(f"Error reading {item}: {e}")

    return result_row

def get_depth(d):
    if not isinstance(d, dict) or not d:
        return 0
    return 1 + max(get_depth(v) for v in d.values())

def generate_summary(output_data):
    result = {}
    rows = []
    Grouptype = "Scores"

    # convert into desired flat structure
        #    dbname Grouptype              module     score
        # 0  analysis    Scores    accuracy scoring  81.91270
        # 1  analysis    Scores        completeness  90.16665
        # 2   archive    Scores        completeness  97.56510
        # 3   archive    Scores  uniqueness scoring  99.99210
   
    if get_depth(output_data) >= 3:
        for dbname, modules in output_data.items():
            for module, tables in modules.items():
                for tbl, score in tables.items():
                    rows.append([dbname, Grouptype, module, tbl, score])

        df = pd.DataFrame(rows, columns=["dbname", "Grouptype", "module", "table", "score"])

        # group by dbname and module
        df_grouped = df.groupby(["dbname", "Grouptype","module"])["score"].mean().reset_index()
        df_grouped["score"] = df_grouped["score"].round(4)
        
        if "dbname".lower() in df_grouped.columns.str.lower():
            for _, row in df_grouped.iterrows():
                db = row["dbname"]
                group = row["Grouptype"]
                module = row["module"]
                score = row["score"]

                result.setdefault(db, {}).setdefault(group, {})[module] = score

    else:
        print("Unexpected structure, skipping summary")

    return result

def main_block(root_path,outputfile,summaryfile,column_mapping):

    rev_column_mapping = generate_column_mapping(column_mapping)

    final_output = process_folder(root_path,rev_column_mapping)
    # print dict to pretty json
    with open(outputfile, "w") as f:
        json.dump(final_output, f, indent=4) 
   
    summary = generate_summary(final_output)
    with open(summaryfile, "w") as f:
        json.dump(summary, f, indent=4)
   


# Set variables for program execution like path and file names
column_mapping = {
    "finalscore": ["completescore", "totalscore", "uniquenessscore"]
}

rootfolderpath = "C:/WorkSakshi/Python/dump/belden"
summaryfilename = "C:/WorkSakshi/Python/data/dumpOutput/db_score.json"
consolidatedfilename = "C:/WorkSakshi/Python/data/dumpOutput/table_score.json"

main_block(rootfolderpath,consolidatedfilename,summaryfilename,column_mapping)