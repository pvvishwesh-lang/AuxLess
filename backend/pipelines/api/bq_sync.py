import logging
from datetime import datetime
from google.cloud import bigquery, firestore

logger = logging.getLogger(__name__)

WEIGHTS = {
    "like":    +2.0,
    "dislike": -2.0,
    "skip":    -0.5,
    "replay":  +1.5,
}

DATASET = "song_recommendations"


def sync_session_to_bigquery(
    session_id: str,
    project_id: str,
    database_id: str,
    user_ids: list[str] = None
):
    db = firestore.Client(project=project_id, database=database_id)
    bq = bigquery.Client(project=project_id)
    tracks_ref = (
        db.collection("sessions")
        .document(session_id)
        .collection("tracks")
    )
    track_docs = list(tracks_ref.stream())

    if not track_docs:
        logger.info(f"No track data in Firestore for session {session_id}, skipping BQ sync.")
        return

    logger.info(f"Syncing {len(track_docs)} tracks from session {session_id} to BigQuery")
    history_rows = []
    user_preferences = {}  
    if user_ids:
        for uid in user_ids:
            user_preferences[uid] = {
                "liked": set(),
                "disliked": set(),
                "skipped": set(),
                "replayed": set(),
            }
    for doc in track_docs:
        data = doc.to_dict()
        video_id = data.get("video_id", doc.id)
        ts = data.get("last_updated")

        if isinstance(ts, datetime):
            ts_str = ts.isoformat()
        elif ts:
            ts_str = str(ts)
        else:
            ts_str = datetime.utcnow().isoformat()

        for action in ["like", "dislike", "skip", "replay"]:
            count = data.get(f"{action}_count", 0)
            if count <= 0:
                continue

            for _ in range(count):
                history_rows.append({
                    "session_id":  session_id,
                    "user_id":     "session_aggregate",
                    "video_id":    video_id,
                    "action":      action,
                    "score_delta": WEIGHTS.get(action, 0.0),
                    "timestamp":   ts_str,
                })
            action_to_pref = {
                "like": "liked",
                "dislike": "disliked",
                "skip": "skipped",
                "replay": "replayed",
            }
            pref_key = action_to_pref.get(action)
            for uid in user_preferences:
                if pref_key:
                    user_preferences[uid].add(video_id) if False else None
                    user_preferences[uid][pref_key].add(video_id)
    if history_rows:
        history_table = f"{project_id}.{DATASET}.session_history"
        errors = bq.insert_rows_json(history_table, history_rows)
        if errors:
            logger.error(f"BQ session_history insert errors: {errors}")
        else:
            logger.info(
                f"Inserted {len(history_rows)} rows into {history_table}"
            )
    users_table = f"{project_id}.{DATASET}.users"

    for uid, prefs in user_preferences.items():
        _merge_user_preferences(bq, users_table, uid, prefs)

    logger.info(f"BQ sync complete for session {session_id}")


def _merge_user_preferences(
    bq: bigquery.Client,
    table_id: str,
    user_id: str,
    prefs: dict
):
    liked    = list(prefs.get("liked", set()))
    disliked = list(prefs.get("disliked", set()))
    skipped  = list(prefs.get("skipped", set()))
    replayed = list(prefs.get("replayed", set()))

    query = f"""
    MERGE `{table_id}` AS target
    USING (
        SELECT
            @user_id AS user_id,
            @liked AS liked,
            @disliked AS disliked,
            @skipped AS skipped,
            @replayed AS replayed
    ) AS source
    ON target.user_id = source.user_id
    WHEN MATCHED THEN UPDATE SET
        liked_songs    = (
            SELECT ARRAY_AGG(DISTINCT vid)
            FROM UNNEST(ARRAY_CONCAT(IFNULL(target.liked_songs, []), source.liked)) AS vid
        ),
        disliked_songs = (
            SELECT ARRAY_AGG(DISTINCT vid)
            FROM UNNEST(ARRAY_CONCAT(IFNULL(target.disliked_songs, []), source.disliked)) AS vid
        ),
        skipped_songs  = (
            SELECT ARRAY_AGG(DISTINCT vid)
            FROM UNNEST(ARRAY_CONCAT(IFNULL(target.skipped_songs, []), source.skipped)) AS vid
        ),
        replayed_songs = (
            SELECT ARRAY_AGG(DISTINCT vid)
            FROM UNNEST(ARRAY_CONCAT(IFNULL(target.replayed_songs, []), source.replayed)) AS vid
        ),
        last_updated   = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        user_id, liked_songs, disliked_songs, skipped_songs, replayed_songs, last_updated
    ) VALUES (
        source.user_id, source.liked, source.disliked, source.skipped, source.replayed, CURRENT_TIMESTAMP()
    )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ArrayQueryParameter("liked", "STRING", liked),
            bigquery.ArrayQueryParameter("disliked", "STRING", disliked),
            bigquery.ArrayQueryParameter("skipped", "STRING", skipped),
            bigquery.ArrayQueryParameter("replayed", "STRING", replayed),
        ]
    )

    try:
        bq.query(query, job_config=job_config).result()
        logger.info(f"Merged preferences for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to merge preferences for user {user_id}: {e}")
