"""PostgreSQL + pgvector loader."""

import json
import uuid
from pathlib import Path

import polars as pl
import psycopg
from psycopg.rows import dict_row
from tqdm import tqdm

from corpus.config import settings
from corpus.embeddings import generate_embeddings


class PgVectorLoader:
    """Load corpus data into PostgreSQL with pgvector."""

    def __init__(self, dsn: str | None = None) -> None:
        """
        Initialize loader.

        Args:
            dsn: PostgreSQL DSN (default from settings)
        """
        self.dsn = dsn or settings.postgres_dsn

    def create_schema(self) -> None:
        """Create database schema and enable extensions."""
        print("Creating schema...")

        sql_dir = Path(__file__).parent.parent.parent / "sql"

        with psycopg.connect(self.dsn) as conn:
            # Enable extensions
            with open(sql_dir / "001_enable_extensions.sql") as f:
                conn.execute(f.read())

            # Create schema
            with open(sql_dir / "010_schema.sql") as f:
                conn.execute(f.read())

            # Create indexes
            with open(sql_dir / "020_indexes.sql") as f:
                conn.execute(f.read())

            conn.commit()

        print("Schema created successfully")

    def load_data(
        self,
        parquet_path: str | Path,
        embed_provider: str | None = None,
    ) -> None:
        """
        Load data from Parquet into PostgreSQL.

        Args:
            parquet_path: Path to unified.parquet
            embed_provider: Generate embeddings ('openai', 'local', or None)
        """
        print(f"Loading data from {parquet_path}...")

        df = pl.read_parquet(parquet_path)

        # Generate embeddings if requested
        if embed_provider:
            df = generate_embeddings(df, provider=embed_provider)

        print(f"Inserting {len(df)} entities into database...")

        with psycopg.connect(self.dsn) as conn:
            # Create a document record for this load
            doc_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO elden.corpus_document
                (id, source_type, source_uri, title, language)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    doc_id,
                    "curated_corpus",
                    str(parquet_path),
                    "Elden Ring Curated Corpus",
                    "en",
                ),
            )

            # Insert chunks
            for row in tqdm(
                df.iter_rows(named=True), total=len(df), desc="Inserting"
            ):
                chunk_id = str(uuid.uuid4())

                # Parse embedding if present
                embedding = None
                if "embedding" in row and row["embedding"]:
                    if isinstance(row["embedding"], str):
                        embedding = json.loads(row["embedding"])
                    else:
                        embedding = row["embedding"]

                # Parse meta_json
                meta = row.get("meta_json", {})
                if isinstance(meta, str):
                    meta = json.loads(meta)

                # Add sources to meta
                sources = row.get("sources", [])
                if isinstance(sources, str):
                    sources = json.loads(sources)
                meta["sources"] = sources

                conn.execute(
                    """
                    INSERT INTO elden.corpus_chunk
                    (id, document_id, entity_type, game_entity_id, is_dlc,
                     name, text, meta, embedding)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        chunk_id,
                        doc_id,
                        row["entity_type"],
                        row["slug"],
                        row["is_dlc"],
                        row["name"],
                        row["description"],
                        json.dumps(meta),
                        embedding,
                    ),
                )

            conn.commit()

        print("Data loaded successfully")

    def query_similar(
        self, text: str, limit: int = 10, entity_type: str | None = None
    ) -> list[dict[str, object]]:
        """
        Query similar entities using vector similarity.

        Args:
            text: Query text
            limit: Number of results
            entity_type: Filter by entity type

        Returns:
            List of similar entities
        """
        # Generate embedding for query
        query_df = pl.DataFrame({"description": [text]})
        query_df = generate_embeddings(
            query_df, provider=settings.embed_provider
        )
        query_embedding = json.loads(
            query_df.select("embedding").item(0, 0)
        )

        with psycopg.connect(
            self.dsn, row_factory=dict_row
        ) as conn:
            if entity_type:
                results = conn.execute(
                    """
                    SELECT name, entity_type, text, is_dlc,
                           embedding <-> %s::vector AS distance
                    FROM elden.corpus_chunk
                    WHERE entity_type = %s AND embedding IS NOT NULL
                    ORDER BY distance
                    LIMIT %s
                    """,
                    (query_embedding, entity_type, limit),
                ).fetchall()
            else:
                results = conn.execute(
                    """
                    SELECT name, entity_type, text, is_dlc,
                           embedding <-> %s::vector AS distance
                    FROM elden.corpus_chunk
                    WHERE embedding IS NOT NULL
                    ORDER BY distance
                    LIMIT %s
                    """,
                    (query_embedding, limit),
                ).fetchall()

        return results


def load_to_postgres(
    dsn: str, parquet_path: str | Path, create: bool = True, embed: bool = False
) -> None:
    """
    Load corpus to PostgreSQL.

    Args:
        dsn: PostgreSQL DSN
        parquet_path: Path to unified.parquet
        create: Create schema first
        embed: Generate embeddings
    """
    loader = PgVectorLoader(dsn)

    if create:
        loader.create_schema()

    embed_provider = settings.embed_provider if embed else None
    loader.load_data(parquet_path, embed_provider)
