from dotenv import load_dotenv
load_dotenv()
import os
from crewai import Crew
from textwrap import dedent
from .config.agents import DataAgents
from .tasks import Tasks
from pathlib import Path
from puredata.tools.dataloader_tool import state
from puredata.database.dynamic_sql import save_df_as_new_table
from datetime import datetime
import re



class DataCrew:
    def __init__(self, target_columns, Data_File_Path):
        self.target_columns = target_columns
        self.Data_File_Path = Data_File_Path

    def run(self):
        # agents and tasks imports
        agents = DataAgents()
        tasks = Tasks()

        # Agent Definition
        engineer = agents.engineer()
        analyst = agents.analyst()
        scientist = agents.scientist()

        # Task Defintion
        IngestingData = tasks.ingest_and_clean_data(
            engineer,
            self.target_columns,
            self.Data_File_Path,
        )

        # Crew Definition
        crew = Crew(
            agents=[engineer],  # Update all agents here
            tasks=[IngestingData],  # Update Tasks here
            verbose=True,
        )
        result = crew.kickoff()
        return result


def getRoot() -> Path:
    return Path(__file__).resolve().parents[2]


# main function
if __name__ == "__main__":
    print("Initial Print")
    print("-------------------------------")
    filename = input(dedent("""Enter your data file name:  """)).strip()
    rootpath = getRoot()
    FILEPATH = (rootpath / "Datafiles" / "Raw" / filename).resolve()
    print(f"DEBUG file_path = {FILEPATH}")

    columns = input(dedent("""Enter the columns you want to target: """)).strip()
    target_columns = [c.strip() for c in columns.split(",") if c.strip()]
    crew = DataCrew(target_columns=target_columns, Data_File_Path=str(FILEPATH))
    result = crew.run()

    if state.df is None:
        print("No cleaned dataframe found in memory (state.df is None).")
    else:
        print("Rows in cleaned dataframe:", len(state.df))
        
        # Clickhouse Database Save
        ts = datetime.now().strftime("%H%M%S") # Time Gen
        file_stem = Path(filename).stem

        # make a readable cols part
        cols_part = "_".join([c.strip().lower().replace(" ", "_") for c in target_columns]) # replaces spaces with _ for Clickhouse
        cols_part = re.sub(r"[^a-z0-9_]+", "_", cols_part)

        table_name = f"{file_stem}__{cols_part}__{ts}".lower() # Table Name generation based on Time

        # Dynamic Storage Access
        ch_info = save_df_as_new_table(
            df=state.df,
            source_file=filename,
            table_name=table_name
        )

        print(f"Saved to ClickHouse: {ch_info['database']}.{ch_info['table']} ({ch_info['rows']} rows)")
        
        # Local Save for Clean db (easier analysis)
        output_dir = rootpath / "Datafiles" / "Clean"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{Path(filename).stem}_clean.csv"
        state.df.to_csv(output_path, index=False)

        print(f"Saved cleaned data to: {output_path}")

    print(result)
