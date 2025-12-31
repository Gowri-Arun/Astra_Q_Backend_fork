from enum import Enum

class Mode(str, Enum):
    KG = "kg"
    RAG = "rag"
    BOTH = "both"

def decide_mode(question: str) -> Mode:
    q = question.lower()

    # Very simple heuristics for Phase 5
    if any(phrase in q for phrase in ["where is", "where are", "list", "which products", "which data"]):
        return Mode.KG

    if any(phrase in q for phrase in ["explain", "what is", "how does", "describe", "why"]):
        return Mode.RAG

    # Default: try both, then merge
    return Mode.BOTH
