import pandas as pd

def standardrize_columns(tdf,revcolumn_mapping):
    new_columns = []
        
    for col in tdf.columns:
        clean_col = col.lower().strip().replace('_', '').replace(' ', '')

        new_name = ""
        if clean_col in revcolumn_mapping:
            new_name = (revcolumn_mapping[clean_col])

        # Avoid duplicates. Map key name from first field only if more than one mapped field available
        if new_name != "" and new_name not in new_columns:
            new_columns.append(new_name)
        else:
            new_columns.append(clean_col)  # keep as is
        
    tdf.columns = new_columns
    return tdf

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

# convert the list data into desired json format
#           SAMPLE INPUT
# data = [
#     ['analysis', 'data quality', 'remainingenrichment.csv', 0.7081],
#     ['analysis', 'data quality', 'vnm_bag_customer_match.csv', 0.8646],
#     ['archive', 'accuracy scoring', 'address_92k.csv', 71.2663]
# ]
#           SAMPLE OUTPUT
# {
#   "analysis": {
#     "accuracy scoring": {
#       "file.csv": score  }}
# }

def convert_table_result(file_data):
    result = {}
    
    # setDefault - If the key is already present, it returns its current value; 
    # if not, it inserts the key with a specified default value and returns that value

    for db, module, file, score in file_data:
        result.setdefault(db, {}).setdefault(module, {})[file] = score

    return result


#           SAMPLE INPUT
# data = [
#     ['analysis', 'data quality', 'remainingenrichment.csv', 0.7081],
#     ['analysis', 'data quality', 'vnm_bag_customer_match.csv', 0.8646],
#     ['archive', 'accuracy scoring', 'address_92k.csv', 71.2663]
# ]
#          SAMPLE OUTPUT
#   {
#     "analysis": {
#         "scores": {
#             "accuracy scoring": 88.56,
#             "completeness": 88.99 }}
#   }


# convert the input list data into desired json format
def convert_db_result(config_info,file_data):
    result = {}
    grouping_colname = config_info["processing"]["db_file_grouping"]

    # Convert list into data frame and give column names explicitely
    tdf = pd.DataFrame(file_data,columns=["dbname", "module", "table", "score"])

    # Add extra column scores on second position which is required for summary file 
    # and it's not present in Input csv files
    tdf.insert(1, "grouptype", grouping_colname)


    # group the df by first and second column and drop 3rd columna 
    # Take mean of column 3 (ignore column 2 for aggregation) 
    # groupby([0, 1]) → groups by col 0 & 1 
    # then take mean of 4th column by [3].mean() 
    # then reset index will convert to DF
 
    tdf_grouped = tdf.groupby(["dbname", "grouptype", "module"])["score"].mean().reset_index()
        
    # iterate each row. Row will behave like dict now
    for row in tdf_grouped.itertuples():
        # print(row.dbname, row.grouptype, row.module, row.score)
        tscore = round(row.score,4)

        # setDefault - If the key is already present, it returns its current value; 
        # if not, it inserts the key with a specified default value and returns that value

        result.setdefault(row.dbname,{}).setdefault(row.grouptype,{})[row.module] = tscore
        
    return result