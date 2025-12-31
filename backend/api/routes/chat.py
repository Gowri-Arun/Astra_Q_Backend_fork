from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Literal, Optional

from rag_pipeline.retrieve import run_rag_pipeline
from kg_pipeline.kg_nl_demo import ask_kg
from backend.api.router_logic import decide_mode, Mode

router = APIRouter()

class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str

class ChatRequest(BaseModel):
    user_id: Optional[str] = None
    message: str
    history: List[ChatMessage] = []

class ChatResponse(BaseModel):
    answer: str
    sources: List[dict]
    mode: Mode

@router.post("/chat", response_model=ChatResponse)
async def chat(payload: ChatRequest) -> ChatResponse:
    mode = decide_mode(payload.message)

    kg_result = None
    rag_result = None

    if mode in (Mode.KG, Mode.BOTH):
        kg_result = ask_kg(payload.message)

    if mode in (Mode.RAG, Mode.BOTH):
        rag_result = run_rag_pipeline(payload.message)

    # Simple fusion logic
    if mode == Mode.KG and kg_result:
        return ChatResponse(
            answer=kg_result["answer"],
            sources=[{"source": "KG", "cypher": kg_result["cypher"], "rows": kg_result["rows"]}],
            mode=mode,
        )

    if mode == Mode.RAG and rag_result:
        return ChatResponse(
            answer=rag_result["answer"],
            sources=rag_result["sources"],
            mode=mode,
        )

    # BOTH: concatenate answers + sources for now
    combined_answer_parts = []
    combined_sources: List[dict] = []

    if kg_result:
        combined_answer_parts.append(kg_result["answer"])
        combined_sources.append({"source": "KG", "cypher": kg_result["cypher"]})

    if rag_result:
        combined_answer_parts.append(rag_result["answer"])
        combined_sources.extend(rag_result["sources"])

    return ChatResponse(
        answer="\n\n".join(part for part in combined_answer_parts if part),
        sources=combined_sources,
        mode=mode,
    )
