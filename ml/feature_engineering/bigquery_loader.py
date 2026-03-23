"""
BigQuery loader for song embeddings.
Used by generate_song_embeddings.py to upload
new song embeddings to the song catalog.

All table references come from config.py.
"""

from google.cloud import bigquery
import pandas as pd

from ml.recommendation.config import PROJECT_ID, TABLE_REF


def get_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def ensure_table_exists(client: bigquery.Client, df: pd.DataFrame):
    """
    Creates the song_embeddings table in BigQuery if it doesn't exist.
    """
    try:
        client.get_table(TABLE_REF)
        print(f"Table exists: {TABLE_REF}")
    except Exception:
        print(f"Creating table: {TABLE_REF}")
        schema = [
            bigquery.SchemaField("video_id",        "STRING",  mode="REQUIRED"),
            bigquery.SchemaField("track_title",      "STRING",  mode="NULLABLE"),
            bigquery.SchemaField("artist_name",      "STRING",  mode="NULLABLE"),
            bigquery.SchemaField("genre",            "STRING",  mode="NULLABLE"),
            bigquery.SchemaField("duration_sec",     "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("popularity_score", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("embedding",        "FLOAT64", mode="REPEATED"),
        ]
        client.create_table(bigquery.Table(TABLE_REF, schema=schema))
        print(f"Table created: {TABLE_REF}")


def get_existing_video_ids(client: bigquery.Client) -> set:
    """
    Returns set of video_ids already in BigQuery.
    Used to skip re-embedding songs already in catalog.
    """
    try:
        result = client.query(
            f"SELECT video_id FROM `{TABLE_REF}`"
        ).result()
        ids = {row.video_id for row in result}
        print(f"{len(ids)} songs already in BigQuery.")
        return ids
    except Exception:
        print("Table is empty.")
        return set()


def load_embeddings(client: bigquery.Client, df: pd.DataFrame):
    """
    Inserts new song embeddings into BigQuery.
    Skips songs already present in the catalog.
    """
    print(f"\nStarting load — {len(df)} songs in batch.")

    ensure_table_exists(client, df)
    existing_ids = get_existing_video_ids(client)

    df_new = df[~df["video_id"].isin(existing_ids)].reset_index(drop=True)
    print(f"Skipped {len(df) - len(df_new)} duplicates.")
    print(f"{len(df_new)} new songs to insert.")

    if df_new.empty:
        print("Nothing to insert.\n")
        return

    errors = client.insert_rows_json(TABLE_REF, df_new.to_dict(orient="records"))
    if errors:
        print(f"Errors during insert: {errors}")
    else:
        print(f"Inserted {len(df_new)} songs into BigQuery.\n")