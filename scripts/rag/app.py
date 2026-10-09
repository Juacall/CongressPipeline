# scripts/rag/app.py
from fastapi import FastAPI
from pydantic import BaseModel
from scripts.rag.agent import agent_executor

app = FastAPI(title="Legislative RAG Agent API")

class QueryRequest(BaseModel):
    query: str

@app.post("/chat")
async def chat(request: QueryRequest):
    response = agent_executor.invoke({"messages": [("user", request.query)]})
    # Extracts the final assistant message
    final_message = response["messages"][-1].content
    return {"response": final_message}