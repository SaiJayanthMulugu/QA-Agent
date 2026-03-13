def generate_markdown_report(analysis_json: str, ticket_info: str = "") -> str:
    """
    Use this tool to format the JSON findings and ETL analysis into a comprehensive Markdown report.
    Pass in the generated JSON from the ETL analysis.
    """
    # This could be easily replaced by a true Jinja2 Environment template later!
    report = f"""# Autonomous QA & ETL Architecture Report

## Overview
Analysis successfully completed.

### Context
{ticket_info}

## Deep-Dive Analysis Snapshot
```json
{analysis_json}
```

*Report generated automatically by the Databricks QA Orchestrator.*
"""
    return report
