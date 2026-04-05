import os
import pandas as pd

# -------------------------------
# 1. FIND ALL CSV FILES
# -------------------------------
def get_csv_files(root_dir):
    csv_files = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".csv"):
                csv_files.append(os.path.join(root, file))
    return csv_files


# -------------------------------
# 2. STANDARDIZE COLUMN NAMES
# -------------------------------
def standardize_columns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df


# -------------------------------
# 3. CLEAN DATA
# -------------------------------
def clean_data(df):
    # Handle missing values
    df = df.fillna(method='ffill')  # or df.fillna(0)

    # Remove duplicates
    df = df.drop_duplicates()

    return df


# -------------------------------
# 4. LOAD AND COMBINE DATA
# -------------------------------
def load_and_combine(csv_files):
    dataframes = []

    for file in csv_files:
        try:
            df = pd.read_csv(file)

            df = standardize_columns(df)
            df['source_file'] = os.path.basename(file)

            dataframes.append(df)

        except Exception as e:
            print(f"Error reading {file}: {e}")

    combined_df = pd.concat(dataframes, ignore_index=True)
    return combined_df


# -------------------------------
# 5. GENERATE SUMMARY
# -------------------------------
def generate_summary(df, original_count, file_count):
    summary = {}

    summary['Total Files Processed'] = file_count
    summary['Records Before Cleaning'] = original_count
    summary['Records After Cleaning'] = len(df)

    # Example Aggregations
    numeric_cols = df.select_dtypes(include='number').columns

    if len(numeric_cols) > 0:
        summary['Aggregations'] = df[numeric_cols].agg(['sum', 'mean', 'count']).to_dict()

    return summary


# -------------------------------
# MAIN FUNCTION
# -------------------------------
def main(root_dir, output_file="consolidated_output.csv"):
    csv_files = get_csv_files(root_dir)

    print(f"Found {len(csv_files)} CSV files")

    combined_df = load_and_combine(csv_files)

    original_count = len(combined_df)

    # Clean data
    cleaned_df = clean_data(combined_df)

    # Save output
    cleaned_df.to_csv(output_file, index=False)

    # Generate summary
    summary = generate_summary(cleaned_df, original_count, len(csv_files))

    print("\n📊 SUMMARY REPORT")
    for key, value in summary.items():
        print(f"{key}: {value}")

    print(f"\n✅ Consolidated file saved as: {output_file}")


# -------------------------------
# RUN
# -------------------------------
if __name__ == "__main__":
    root_directory = "C:/WorkSakshi/Python/data/health"
    main(root_directory)