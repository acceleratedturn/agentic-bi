import os
from crewai import Crew

from textwrap import dedent
from .config.agents import DataAgents
from .tasks import Tasks


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
            agents=[engineer], # Update all agents here
            tasks=[IngestingData], # Update Tasks here
            verbose=True,
        )
        result = crew.kickoff()
        return result


# main function
if __name__ == "__main__":
    print("Initial Print")
    print("-------------------------------")
    var1 = input(dedent("""Enter your data file path:  """))
    var2 = input(dedent("""Enter the columns you want to target: """))