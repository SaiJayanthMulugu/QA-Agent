from langchain_core.tools import tool
from typing import Dict

@tool
def fetch_jira_ticket(ticket_id: str) -> Dict[str, str]:
    """
    Use this tool to fetch the description, acceptance criteria, and comments from a Jira ticket.
    """
    # Placeholder for the actual Jira issue parsing logic
    return {
        "ticket_id": ticket_id,
        "description": "As a data analyst, I need the user status column normalized for the new Silver table.",
        "acceptance_criteria": "1. Status 'A' becomes 'Active'. 2. Status 'I' becomes 'Inactive'. 3. Join with reference table based on ID.",
        "comments": "Ensure that the Databricks job properly handles the Left Join."
    }
