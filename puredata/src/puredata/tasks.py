from crewai import Task
from textwrap import dedent



class Tasks:
    def __tip_section(self):
        return "If you do your BEST WORK, I'll give you a $10,000 commission!"

    def ingest_and_clean_data(self, agent, target_columns, file_path: str):
        return Task(
            description=dedent(
                f"""
                ***Task***: Clean up a dataframe to focus on important columns
                
                *** Description ***:
                Ingest the raw data from the following filepath: {file_path}.
                
                Your primary goal is to extract ONLY the specific metrics or columns requested by the user: {target_columns}.
                Perform necessary data cleaning which includes:
                1. Handling missing values or nulls found in the requested columns.
                2. Ensuring data types are correct for analysis (e.g., dates are datetime objects, numbers are floats).
                3. Removing duplicates or outliers that could skew statistical results.
                4. Removing any unnecessary columns

                **Parameters**:
                - Raw Dataframe file path: {file_path}
                - Columns that need to be kept: {target_columns}

                **Note** {self.__tip_section()}
        
                Use the custom DataLoader tool to process the file efficiently to save on tokens.
                Ensure the final output is a strictly structured dataset ready for the Data Analyst.
            """
            ),
            expected_output="A clean, structured Pandas DataFrame containing only the user-defined columns: " + str(target_columns),
            agent=agent,
        )

    def verify_data_readiness(self, agent):
        return Task(
            description=dedent(
                f"""
                **Task**: Review the dataframe you generated
                **Description**: 
                Review the DataFrame generated in the previous task to ensure it is analysis-ready.
                Check that the columns match exactly what was requested and that no corruption occurred during the cleaning phase.
                
                {self.__tip_section()}

                Prepare a summary report of the cleaning actions taken (e.g., '50 nulls removed', 'column X converted to float').
            """
            ),
            expected_output="A validation report confirming the data structure and a summary of the cleaning operations performed.",
            agent=agent,
        )
    