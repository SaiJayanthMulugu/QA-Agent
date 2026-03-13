import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    # Databricks settings
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    
    # Jira settings
    JIRA_SERVER = os.getenv("JIRA_SERVER")
    JIRA_EMAIL = os.getenv("JIRA_EMAIL")
    JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")
    
    # OpenAI/LLM settings
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

config = Config()
