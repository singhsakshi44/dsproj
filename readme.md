# Project Title

Multi-File Data Validation and Consolidation System
Objective: Develop a Python program that reads multiple CSV files stored across different folders, processes their contents, and produces a consolidated dataset along with meaningful insights.


## Description

You are given a root directory that contains multiple subfolders. Each subfolder may contain one or more CSV files with similar or related data structures.
Your task is to:
	1.	Traverse through all folders and subfolders within the given directory.
	2.	Identify and read all CSV files.
	3.	Extract and combine the data from these files into a single unified dataset.
	4.	Handle inconsistencies such as:
		o	Missing values
		o	Different column orders
		o	Duplicate records
	5.	Perform basic data processing and aggregation.
	6.	Present the consolidated output in a structured format.
	
	Functional Requirements
		•	Recursively scan directories for .csv files
		•	Read CSV files using Python (e.g., initially use pandas then try without using Pandas)
		•	Merge datasets into a single DataFrame
		•	Clean the data:
			o	Standardize column names
			o	Handle null values
			o	Remove duplicates
		•	Generate summary statistics such as:
			o	Total number of records
			o	Aggregated metrics (e.g., sum, average, count based on columns)
			
	Output Requirements
		•	A consolidated CSV file containing all processed data
		•	A summary report printed to the console or saved as a file, including:
			o	Total files processed
			o	Total records before and after cleaning
			o	Key aggregated insights
			
	Constraints
		•	The program should work for any number of folders and CSV files
		•	It should handle large datasets efficiently
		•	Code should be modular and reusable
		
	Optional
		•	Add logging for tracking processed files
		•	Allow user input for directory path
		•	Visualize key insights using charts
		•	Implement parallel processing for faster execution

## Getting Started

### Dependencies

* refer file requirements.txt for all the dependecies

### Installing

* update config/config.json to provide the parameters for program execution
* Open your terminal or command prompt and navigate to your project folder. Run:


```
	python -m venv .<virtual env name>
	python -m venv .vdsenv
```


### Executing program

* From terminak window running in virtual environment. execute

```
	$env:PYTHONPATH="src"; python main.py
```

## Help

```

## Authors

Sakshi Singh
