import os
import sys
import uuid

# In Databricks, we might need to add our local repo path to sys so imports work smoothly
sys.path.append(os.path.dirname(__file__))

from agents.graph import build_graph
from langchain_core.messages import HumanMessage

def run_databricks_job():
    """
    Entry point for running this agent as a Databricks Notebook or Job.
    """
    print("--- Starting Databricks QA Automation Agent ---")
    
    app = build_graph()
    
    # We require a thread ID for memory saving / state tracking
    # If triggered by a Job, this could be the job_run_id
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}
    
    # Since this is a notebook, rather than a continuous chat loop, 
    # we would pass in our structured parameters via dbutils.widgets
    # Example simulated input instead of input() loops:
    simulate_user_input = "Can you run an ETL QA check on the Silver layer? The source is bronze_db.users and the target is silver_db.clean_users."
    
    print(f"Triggering Orchestrator with User Query:\n{simulate_user_input}\n")
    
    for event in app.stream({"messages": [HumanMessage(content=simulate_user_input)]}, config=config, stream_mode="values"):
        last_msg = event["messages"][-1]
        
        if hasattr(last_msg, 'type') and last_msg.type == "ai" and last_msg.content:
             # Just printing the AI responses
             if getattr(last_msg, 'additional_kwargs', {}).get('tool_calls'):
                 print(f"  [Orchestrator Executing Tool: {last_msg.additional_kwargs['tool_calls'][0]['function']['name']}]")
             else:
                 pass
                 
    # In a notebook, to bypass the "Strategic Pause / Approval Gate", 
    # you would resume the graph programmatically if `user_approved_execution` is true,
    # or expose it to a human-in-the-loop review widget natively in Databricks!
    
    final_state = app.get_state(config)
    final_msg = final_state.values["messages"][-1]
    
    if final_msg.type == "ai" and final_msg.content:
         print("--- Current State / Output ---")
         print(final_msg.content)
         
if __name__ == "__main__":
    run_databricks_job()
