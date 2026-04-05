
import os
import pandas as pd
from functools import reduce


column_mapping = {
    "uniq_id": ["src_sys_id", "sfdcid", "erp_src_sys_id"],
    "global_id": ["erp_global", "sfdc_global"],
    "similar_score": ["sim_s"]
}

master_df = pd.DataFrame()

def create_col_mapping(mapping):
    revcolumn_mapping = {}

    for standard, variations in mapping.items():
        for col in variations:
            revcolumn_mapping[col.lower().strip()] = standard

    print(revcolumn_mapping)
    return revcolumn_mapping

def standardrize_columns(tdf,revcol_mapping):
    new_columns = []
    new_colname = ""
    
    for col in tdf.columns:
        clean_col = col.lower().strip()

        if clean_col in revcol_mapping:
            new_colname = (revcol_mapping[clean_col])
        else:
            new_colname = clean_col

        if new_colname not in new_columns:
            new_columns.append(new_colname)
        else:
            new_columns.append(clean_col)  # keep as is
        
    tdf.columns = new_columns

    return tdf

def read_dir(rootpath):

    filepathlist = []

    # OS.walk is fastest than pathlib or glob
    for dirpath,dirname,filesname in os.walk(rootpath):
        # print(f"1= {dirpath} 2= {dirname} 3= {filename}")
        # filepathlist = [file for file in filesname if ".csv" in file]

        for file in filesname:
            if file.endswith(".csv"):
                filepathlist.append(os.path.join(dirpath, file))

    # print(filepathlist)
    return filepathlist

def check_file_columns(filenamelist):
    
    all_columns = []
    summ_data = []
    # Extract all column sets
    
    all_columns = [set(pd.read_csv(f, nrows=0).columns) for f in filenamelist]
    # all_columns.append(pd.read_csv(f, nrows=0).columns for f in filenamelist)
    for f in filenamelist:
        tdf = pd.read_csv(f, nrows=0)
        cols = ", ".join(tdf.columns)
        file_name = os.path.basename(f)
        summ_data.append({
            "file_name": file_name,
            "columns": cols
        })
    
    return summ_data
   
    # # Find intersection across all sets
    # common_to_all = reduce(set.intersection, all_columns)
    # print(common_to_all)
    # print(len(common_to_all))
    # print(f"Columns in every file: {common_to_all}")


def read_files(filenamelist,rev_col_mapping):
    datafrms = []
    for file in filenamelist:
        tmpdf = pd.read_csv(
                            file,
                            on_bad_lines='skip',   # skips corrupted rows
                            quotechar='"',
                            low_memory=False
        )
        tmpdf = standardrize_columns(tmpdf,rev_col_mapping)
        tmpdf['source_file'] = os.path.basename(file)
        datafrms.append(tmpdf)

    combined_df = pd.concat(datafrms, ignore_index=True)
    return combined_df


def data_cleaning(ipdf):
    # print(ipdf.info())  

    # remove rows with null in key cols
    ipdf.dropna(subset=['age','sex'],inplace=True)

    #Remove any duplicates
    ipdf.drop_duplicates(inplace=True)    

    # fill data in empty cell with average value
    chol_meanval = int(ipdf["chol"].mean())    
    ipdf.fillna({"chol":chol_meanval},inplace=True)

    # Clean wrong data-Either renove it ot fix data like define rule based on columns
    for x in ipdf.index:
        if ipdf.loc[x,"cp"] > 3:
            ipdf.loc[x,'cp'] = 3
    
    return ipdf


def data_analysis(ipfdf,rec_count,file_count,):
     
    # with open("C:/WorkSakshi/Python/data/healthOutput/corrfile.csv", "w") as f:
    #     f.write(ipcldf.corr(numeric_only=True).to_string())

    file_summary = {}
    file_summary['Total Files Processed'] = file_count
    file_summary['Records Before Cleaning'] = rec_count
    file_summary['Records After Cleaning'] = len(ipfdf)
    
    # file_summary['Average Patient Age'] = int(ipfdf['age'].mean())
    # filtered_ds = ipfdf.query('(cp == 0 & exang == 1) or oldpeak > 2')
    # file_summary['Persons with possibility of heart attack'] = len(filtered_ds)

    # # Example Aggregations
    # numeric_cols = ipfdf.select_dtypes(include='number').columns

    # if len(numeric_cols) > 0:
    #     file_summary['Aggregations'] = ipfdf[numeric_cols].agg(['sum', 'mean', 'count']).to_dict()

    return file_summary

def mainblk(rootpath,summaryfile,outputfile):
    revcolumn_mapping = create_col_mapping(column_mapping)
    files_to_scan = read_dir(rootpath)
    cols_data = check_file_columns(files_to_scan)
    mergeddf = read_files(files_to_scan,column_mapping)
    merged_cnt = len(mergeddf)
    #print(merged_cnt)
    # print(mergeddf.info()) 

    my_df = pd.DataFrame(cols_data)

    # Save to CSV
    my_df.to_csv(outputfile, index=False)

    with open(summaryfile, "w") as f:
        mergeddf.info(buf=f)
    
    # cleanmasterdf = data_cleaning(mergeddf)
    # summary = data_analysis(cleanmasterdf,merged_cnt,len(files_to_scan))
    #  # file containing all processed data
    # cleanmasterdf.to_csv(outputfile)

    # with open(summaryfile, "w") as f:
    #     for key, value in summary.items():
    #         f.write(f"{key}, {value}\n")



rootfolderpath = "C:/WorkSakshi/Python/data/1"
summaryfilename = "C:/WorkSakshi/Python/data/dumpOutput/summ.csv"
consolidatedfilename = "C:/WorkSakshi/Python/data/dumpOutput/AllData.csv"
mainblk(rootfolderpath,summaryfilename,consolidatedfilename)