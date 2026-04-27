"""Query interface for the GHG Protocol Scope 3 Standard RAG index."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import chromadb
from sentence_transformers import SentenceTransformer

RAG_DB_PATH = Path(__file__).parent / "ghg_index"
COLLECTION_NAME = "ghg_scope3_standard"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"


class IndexNotBuiltError(Exception):
    """Raised when the ChromaDB index has not been built yet."""


@dataclass
class RetrievalResult:
    text: str
    chapter_num: int
    chapter_title: str
    section_num: str
    section_title: str
    start_page: int
    end_page: int
    category_num: int
    category_name: str
    topic_tags: list[str]
    source_citation: str
    distance: float


@lru_cache(maxsize=1)
def _get_model() -> SentenceTransformer:
    return SentenceTransformer(EMBEDDING_MODEL)


def retrieve(
    query: str,
    n_results: int = 5,
    db_path: Path = RAG_DB_PATH,
) -> list[RetrievalResult]:
    """Embed query and return the top-n matching chunks with full citation metadata.

    Raises IndexNotBuiltError if the index has not been built.
    """
    if not db_path.exists():
        raise IndexNotBuiltError(
            f"RAG index not found at {db_path}. Run `python -m rag.ingest` to build it."
        )

    client = chromadb.PersistentClient(path=str(db_path))

    try:
        collection = client.get_collection(COLLECTION_NAME)
    except Exception:
        raise IndexNotBuiltError(
            f"Collection '{COLLECTION_NAME}' not found. Run `python -m rag.ingest` to build it."
        )

    if collection.count() == 0:
        raise IndexNotBuiltError(
            "RAG index is empty. Run `python -m rag.ingest` to populate it."
        )

    model = _get_model()
    query_embedding = model.encode(query).tolist()

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=min(n_results, collection.count()),
        include=["documents", "metadatas", "distances"],
    )

    retrieval_results: list[RetrievalResult] = []
    docs = results["documents"][0]
    metas = results["metadatas"][0]
    distances = results["distances"][0]

    for doc, meta, dist in zip(docs, metas, distances):
        retrieval_results.append(
            RetrievalResult(
                text=doc,
                chapter_num=meta.get("chapter_num", 0),
                chapter_title=meta.get("chapter_title", ""),
                section_num=meta.get("section_num", ""),
                section_title=meta.get("section_title", ""),
                start_page=meta.get("start_page", 0),
                end_page=meta.get("end_page", 0),
                category_num=meta.get("category_num", 0),
                category_name=meta.get("category_name", ""),
                topic_tags=meta.get("topic_tags", "").split("|") if meta.get("topic_tags") else [],
                source_citation=meta.get("source_citation", ""),
                distance=dist,
            )
        )

    return retrieval_results
