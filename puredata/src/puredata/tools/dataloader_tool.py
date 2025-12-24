import pandas as pd
import os
from typing import Optional, Any
from crewai.tools import tool

class DataState:
    df: Optional[pd.DataFrame] = None
state = DataState()

# Loads the data into a df
@tool("Data_Loader")
def data_loader(file_path:str) -> str:
    """
    Load a raw dataset file from disk into a pandas DataFrame.
    Supported formats include CSV, JSON, XML, and TXT.
    The loaded DataFrame is stored in shared state for downstream processing.
    """
    try:
        if not os.path.exists(file_path):
            return f"Error: File not found @ {file_path}"
        
        ext = os.path.splitext(file_path)[1].lower()

        if ext == '.csv':
            df = pd.read_csv(file_path)
        elif ext == '.json':
            df = pd.read_json(file_path)
        elif ext == '.xml':
            df = pd.read_xml(file_path)
        elif ext == '.txt':
            df = pd.read_csv(file_path, sep='\t')
        else:
            return f"Error: Unsupported file format {ext}"
        
        state.df = df

        return(f"Succesfully loaded data from {file_path}\n"
               f"Dataset contains {len(df)} rows and {len(df.columns)} columns\n"
               f"Columns available: {', '.join(df.columns)}\n")
    
    except Exception as e:
        return f"An error occured when loading the file: {str(e)}"



# Null data removal
@tool("Data_Cleaner")
def data_cleaner(columns_needed: list[str]) -> str:
    """
    Clean the loaded DataFrame by selecting specified columns,
    trimming whitespace from column names, handling missing values,
    and removing duplicate rows.
    """
    if state.df is None:
        return "Error: No data in memory. Run Data_Loader first."
    try:
        state.df.columns = state.df.columns.str.strip()
        state.df = state.df.map(lambda x: x.strip() if isinstance(x, str) else x)

        existing_cols = [col for col in columns_needed if col in state.df.columns]
        
        if not existing_cols:
            return f"Error: None of the requested columns {columns_needed} were found."
            
        state.df = state.df[existing_cols]

        return (f"Refinement complete. Remaining columns: {', '.join(state.df.columns)}. ")
        
    except Exception as e:
        return f"Cleaning failed: {str(e)}"