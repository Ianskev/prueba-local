from pydantic import BaseModel

class Query(BaseModel):
    query: str
    limit: int = 50
    offset: int = 0
    
class QueryResult(BaseModel):
    data: dict
    total: int
    message: str = None
    execution_time: float
