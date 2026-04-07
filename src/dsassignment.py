
import os
import pandas as pd
import json


column_mapping = {
    "finalscore": ["completescore", "totalscore", "uniquenessscore"]
}

revcolumn_mapping = {}

for standard, variations in column_mapping.items():
    for col in variations:
        revcolumn_mapping[col.lower().strip()] = standard

master_df = pd.DataFrame()
 

def standardrize_columns(tdf):
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
    # print(new_columns)
    return tdf

def process_folder(folderpath):
    db_result = {}

    for item in os.listdir(folderpath):
        item_path = os.path.join(folderpath, item)
        
        if os.path.isdir(item_path):
            res = process_db_folder(item_path)
            
            print(f"{item}   {res}")
            # print("final")
            # print(res["Scoring"])

            db_result[item] = res.get("Scoring","")

    return db_result


def process_db_folder(folderpath:str,basepath:str=None):
    result_row = {}

    # store base root path
    basepath = folderpath if basepath == None else basepath
    # print(f"{basepath}   {folderpath}")

    for item in os.listdir(folderpath):
        item_path = os.path.join(folderpath, item)

        # if folder go one more level down
        if os.path.isdir(item_path):
            subfolder_result = process_db_folder(item_path,basepath)
            
            # sub folfder will have blank result if not valid candidate to read
            if subfolder_result:
                result_row[item] = subfolder_result

        # read csv from scoring folder only
        elif item.endswith(".csv") and "Scoring" in os.path.normpath(folderpath).split(os.sep):
            try:
                tdf = pd.read_csv(item_path)
                tdf = standardrize_columns(tdf)
                score = float(tdf["finalscore"].mean())
                result_row[item] = round(score, 4)

            except Exception as e:
                print(f"Error reading {item}: {e}")

    return result_row

def newmain(root_path,outputfile,summaryfile):
    final_output = process_folder(root_path)

    with open(outputfile, "w") as f:
        json.dump(final_output, f, indent=4)  # 'indent' makes it human-readable

    print(final_output)

    rows = []
    for dbname, modules in final_output.items():
        for module, tables in modules.items():
            for tbl, score in tables.items():
                rows.append([dbname, module, tbl, score])

    df = pd.DataFrame(rows, columns=["dbname", "module", "table", "score"])

    print(df)

# Set path and file names
rootfolderpath = "C:/WorkSakshi/Python/data/1"
summaryfilename = "C:/WorkSakshi/Python/data/dumpOutput/summ.csv"
consolidatedfilename = "C:/WorkSakshi/Python/data/dumpOutput/AllData.csv"
# mainblk(rootfolderpath,consolidatedfilename,summaryfilename)
newmain(rootfolderpath,consolidatedfilename,summaryfilename)