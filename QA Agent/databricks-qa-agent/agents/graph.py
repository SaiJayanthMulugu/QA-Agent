from typing import List, Literal, Optional, Annotated, TypedDict, Any
import operator
import json
from pydantic import BaseModel, Field

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import ToolNode

# Local imports
from agents.prompts import MASTER_SYSTEM_PROMPT, ORCHESTRATOR_SYSTEM_PROMPT
from tools.databricks_tool import fetch_databricks_metadata
from tools.jira_tool import fetch_jira_ticket
from templates.report_generator import generate_markdown_report

# --- Pydantic Models for Structured Output ---

class JoinInfo(BaseModel):
    type: str = Field(description="The join type (Inner, Left, Right, Full)")
    keys: List[str] = Field(description="The specific keys used for the join")
    upstream_tables: List[str] = Field(description="The upstream tables involved in the join")

class Summary(BaseModel):
    detected_joins: List[JoinInfo] = Field(default_factory=list)
    transformation_complexity: Literal["Low", "Medium", "High"] = Field(
        description="Complexity of the transformation"
    )

class ColumnAnalysis(BaseModel):
    column_name: str = Field(description="Name of the column in the target table")
    category: Literal["Direct Mapping", "Transformation", "Calculated Column", "Business Flag"] = Field(
        description="Category of the column"
    )
    logic_description: str = Field(description="Natural language explanation of the transformation logic")
    validation_sql: str = Field(description="High-fidelity Spark SQL validation query")

class FunctionalScenario(BaseModel):
    scenario_name: str = Field(description="Name of the test scenario")
    description: str = Field(description="Description of the test scenario")
    acceptance_criteria: str = Field(description="Acceptance criteria to consider the scenario passed")

class QAAnalysisModel(BaseModel):
    summary: Summary
    column_analysis: List[ColumnAnalysis]
    functional_scenarios: List[FunctionalScenario]


# --- State Definition ---

class QAWorkflowState(TypedDict):
    """
    The state structure that LangGraph will pass around nodes.
    Variables managed by orchestrator.
    """
    messages: Annotated[List[BaseMessage], add_messages]
    
    # State flags
    workflow_type: Optional[str]  # "ETL" or "Functional"
    
    # ETL Specifics
    layer: Optional[str]
    source_path: Optional[str]
    target_path: Optional[str]
    
    # Functional Specifics
    jira_ticket_id: Optional[str]
    user_credentials_provided: bool
    
    # Discovery & Checkpointing
    metadata: Optional[dict]  # Stored databricks metadata
    discovery_complete: bool
    user_approved_execution: bool
    
    # Final outputs
    analysis_results: Optional[Any]


# --- Nodes ---

tools = [fetch_databricks_metadata, fetch_jira_ticket]

def orchestrator_node(state: QAWorkflowState):
    """Main routing and conversational agent"""
    llm = ChatOpenAI(model="gpt-4o", temperature=0)
    llm_with_tools = llm.bind_tools(tools)
    
    sys_msg = SystemMessage(content=ORCHESTRATOR_SYSTEM_PROMPT)
    messages = [sys_msg] + state["messages"]
    
    response = llm_with_tools.invoke(messages)
    return {"messages": [response]}


def analyze_logic_node(state: QAWorkflowState):
    """ETL logic analysis node with strict structured JSON output."""
    llm = ChatOpenAI(model="gpt-4o", temperature=0) 
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", MASTER_SYSTEM_PROMPT),
        ("human", "Analyze the transition from {source_schemas} to {target_schema} in the {layer} layer.")
    ])
    
    chain = prompt | llm.with_structured_output(QAAnalysisModel)
    
    # In a real scenario we'd safeguard against missing metadata here
    metadata = state.get('metadata', {})
    
    response = chain.invoke({
        "layer": metadata.get('layer', 'Unknown'),
        "source_schemas": metadata.get('source_schemas', 'Unknown'),
        "target_schema": metadata.get('target_schema', 'Unknown'),
        "ddl_logic": metadata.get('ddl', 'Unknown')
    })
    
    return {"analysis_results": response}


def should_route_to_analysis(state: QAWorkflowState) -> str:
    """
    Custom router.
    - If last message is a tool call, route to tools.
    - If user gave approval (says 'yes' 'proceed' after discovery), route to ETL Architect Node.
    - Otherwise, go back to user (END).
    """
    messages = state["messages"]
    last_message = messages[-1]
    
    if last_message.tool_calls:
        return "tools"
    
    # Simple heuristic for Human-in-the-loop approval
    if len(messages) >= 2:
        user_msg = messages[-1].content.lower()
        if type(messages[-1]) == HumanMessage and ("yes" in user_msg or "proceed" in user_msg or "approve" in user_msg):
            has_metadata = any([m.type == 'tool' and m.name == 'fetch_databricks_metadata' for m in messages])
            if has_metadata:
                return "etl_architect"
            
    return END

def update_metadata_from_tools_node(state: QAWorkflowState):
    """A silent node that inspects tool messages and updates the structured state object."""
    messages = state["messages"]
    last_message = messages[-1]
    
    metadata = state.get("metadata", {})
    if last_message.type == "tool" and last_message.name == "fetch_databricks_metadata":
        try:
            tool_data = json.loads(last_message.content)
            metadata = tool_data
        except:
             pass
    
    return {"metadata": metadata}

def generate_report_node(state: QAWorkflowState):
    """Final node that uses the reporter tool to write out the markdown."""
    analysis = state.get("analysis_results")
    if analysis:
        analysis_json = analysis.model_dump_json(indent=2)
        report_content = generate_markdown_report(analysis_json, "ETL Automated Run")
        final_msg = AIMessage(content=f"I have successfully completed the rigorous ETL testing suite generation.\n\nHere is your report:\n\n{report_content}")
        return {"messages": [final_msg]}
    
    return {"messages": [AIMessage(content="Could not generate report due to missing analysis.")]}


# --- Graph Construction ---

def build_graph():
    graph = StateGraph(QAWorkflowState)
    
    # Add nodes
    graph.add_node("orchestrator", orchestrator_node)
    graph.add_node("tools", ToolNode(tools))
    graph.add_node("extract_state", update_metadata_from_tools_node)
    graph.add_node("etl_architect", analyze_logic_node)
    graph.add_node("generate_report", generate_report_node)
    
    # Add edges
    graph.add_edge(START, "orchestrator")
    
    graph.add_conditional_edges(
        "orchestrator",
        should_route_to_analysis,
        {
            "tools": "tools",
            "etl_architect": "etl_architect",
            END: END
        }
    )
    
    graph.add_edge("tools", "extract_state")
    graph.add_edge("extract_state", "orchestrator")
    
    graph.add_edge("etl_architect", "generate_report")
    graph.add_edge("generate_report", END)
    
    # Checkpointer for Human-in-the-Loop Memory Pauses
    memory = MemorySaver()
    app = graph.compile(checkpointer=memory) 
    
    return app
