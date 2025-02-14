from dataclasses import dataclass
from typing import Optional
import pandas as pd

@dataclass
class WorkflowResult:
    """Result of a workflow execution"""
    success: bool
    data: Optional[pd.DataFrame] = None
    error: Optional[str] = None 