from crewai import Agent
from textwrap import dedent
from langchain_openai import OpenAI
from langchain_community.llms import Ollama
from langchain_openai import ChatOpenAI


class DataAgents:
    # Implement QWEN2 and Gemini
    def __init__(self):
        self.dataCleaning_OpenAIGPT35 = ChatOpenAI(model="gpt-3.5-turbo", temperature=0.0) # 0.0-0.2 Better for data handelling
        self.OpenAIGPT35 = ChatOpenAI(model="gpt-3.5-turbo", temperature=0.5) # Temp Balance of natural flow and coherence
        self.OpenAIGPT4 = ChatOpenAI(model="gpt-4", temperature=0.9) # Temp Provides unique ideas, and personality

    def engineer(self):
        return Agent(
            role="Data Engineer",
            backstory=dedent(f"""Expert in reading large data through a pandas dataframe, I have decades of experience with cleaning up rough data to return a clean dataframe"""),
            
            goal=dedent(f""" Create a clean dataframe from an input by Ingesting a raw pandas datafram, performing user-defined cleaning, and transforming it 
                        into a structured, analysis-ready dataframe"""),
             tools=[
              ],
            verbose=True,
            llm=self.dataCleaning_OpenAIGPT35,
        )

    def analyst(self):
        return Agent(
            role="Data Analyst",
            backstory=dedent(f"""Expert in analyzing data from a pandas dataframe, I have decades of experience with creating reports summarizing data """),
            goal=dedent(f"""Create a report by Exploring the structured data, performing statistical analysis, and
                            generating clear, descriptive reports and visualizations."""),
            verbose=True,
            llm=self.OpenAIGPT35,
        )
    
    def scientist(self):
        return Agent(
            role="Data Scientist",
            backstory=dedent(f"""Expert in predicting, forecasting, and developing business strategies. Decades of experience in 
                             making accurate predictions and creating methods that provide a net positive that are also actionable"""),
            goal=dedent(f"""Create a report by applying advanced techniques, making predictions, forecasting, and
                            translating findings into actionable business strategy."""),
            verbose=True,
            llm=self.OpenAIGPT35,
            )