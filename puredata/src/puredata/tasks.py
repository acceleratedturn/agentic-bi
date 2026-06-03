from crewai import Task
from textwrap import dedent



class Tasks:

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
                Prepare a summary report of the cleaning actions taken (e.g., '50 nulls removed', 'column X converted to float').
            """
            ),
            expected_output="A validation report confirming the data structure and a summary of the cleaning operations performed.",
            agent=agent,
        )
    
    def analyze_data_summary(self,agent, database:str, table:str,target_columns: list[str]):
        return Task(
            agent=agent,
            description=dedent(f"""
            You are given a ClickHouse table that was produced by Agent 1.

            Database: {database}
            Table: {table}
            Columns: {target_columns}

            Goal: produce an in-depth, number-backed analysis summary.

            Your job:
            - Confirm row count
            - Compute summary statistics (min/max/avg where numeric)
            - Identify missing values
            - Provide insights

            IMPORTANT:
            - Use ClickHouse_Read tool
            - Prefer aggregations (COUNT, AVG, quantiles) over pulling the full table.

            INSIGHTS REQUIREMENTS:
            - Provide 8–12 insights (not 5).
            - Avoid generic claims like “healthy amount” unless you justify it with the distribution.
            - Prefer aggregations over pulling full tables.

            ILLUSTRATIVE INSIGHT EXAMPLES (DO NOT REPEAT VERBATIM):

            These examples demonstrate acceptable insight depth and structure.
            They are illustrative only. Generate NEW insights based on the actual columns and values.

            Example 1 (Distribution-based):
            - "The median value of Column_A is X (p50), with the p10–p90 range spanning Y–Z,
            indicating that the majority of records cluster within a relatively narrow band."

            Example 2 (Relationship-based):
            - "Column_A and Column_B show a weak positive correlation (corr ≈ r),
            suggesting that increases in Column_A are associated with modest increases in Column_B,
            but the relationship is not strong enough to imply direct dependence."

            Example 3 (Bucket / segmentation comparison):
            - "Records in the highest bucket of Column_A average X₂ for Column_B,
            compared to X₁ in the lowest bucket, representing an absolute difference of Δ
            and highlighting a meaningful segmentation effect."

            Example 4 (Outlier / edge case):
            - "A small fraction of records (≈P%) exhibit extreme values where Column_B is high
            despite Column_A being low, indicating atypical or edge-case behavior within the dataset."

            RULES:
            - Do NOT reuse these examples or numbers.
            - Your insights must reference actual computed values from this table.
            """),
            expected_output="A clear analysis summary and key stats from the table, with 8-12 insights based on the data recieved with context of the column names",
        )
    
    def generate_graphs(self,agent):
        return Task(
            description=dedent(
                f"""
                **Task**:
                **Description**:
                **Parameters**:

                **Tools**:
                **Output**:
                """
            ),
            expected_output="enter here",
            agent=agent,
        )

    