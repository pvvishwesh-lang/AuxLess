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
    feedback_ref = (
        db.collection("sessions")
        .document(session_id)
        .collection("user_feedback")
    )
    feedback_docs = list(feedback_ref.stream())

    if not feedback_docs:
        logger.info(f"No user feedback in Firestore for session {session_id}, skipping BQ sync.")
        return

    logger.info(f"Syncing {len(feedback_docs)} feedback entries from session {session_id} to BigQuery")
    history_rows = []
    user_preferences = {}
    for doc in feedback_docs:
        data = doc.to_dict()
        user_id  = data.get("user_id")
        video_id = data.get("video_id")
        action   = data.get("action")
        score_delta = data.get("score_delta", 0.0)
        ts = data.get("last_updated")
        if not user_id or not video_id or not action:
            continue
        if isinstance(ts, datetime):
            ts_str = ts.isoformat()
        elif ts:
            ts_str = str(ts)
        else:
            ts_str = datetime.utcnow().isoformat()
        history_rows.append({
            "session_id":  session_id,
            "user_id":     user_id,
            "video_id":    video_id,
            "action":      action,
            "score_delta": score_delta,
            "timestamp":   ts_str,
        })
        if user_id not in user_preferences:
            user_preferences[user_id] = {
                "liked": set(),
                "disliked": set(),
                "skipped": set(),
                "replayed": set(),
            }
        action_to_pref = {
            "like": "liked",
            "dislike": "disliked",
            "skip": "skipped",
            "replay": "replayed",
        }
        pref_key = action_to_pref.get(action)
        if pref_key:
            user_preferences[user_id][pref_key].add(video_id)
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

    try:
        check_query = f"SELECT user_id FROM `{table_id}` WHERE user_id = @user_id"
        check_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            ]
        )
        rows = list(bq.query(check_query, job_config=check_config).result())

        if rows:
            fetch_query = f"""
            SELECT liked_songs, disliked_songs, skipped_songs, replayed_songs
            FROM `{table_id}` WHERE user_id = @user_id
            """
            fetch_result = list(bq.query(fetch_query, job_config=check_config).result())[0]
            merged_liked = list(set((fetch_result.liked_songs or []) + liked))
            merged_disliked = list(set((fetch_result.disliked_songs or []) + disliked))
            merged_skipped  = list(set((fetch_result.skipped_songs or []) + skipped))
            merged_replayed = list(set((fetch_result.replayed_songs or []) + replayed))
            update_query = f"""
            UPDATE `{table_id}` SET
                liked_songs    = @liked,
                disliked_songs = @disliked,
                skipped_songs  = @skipped,
                replayed_songs = @replayed,
                last_updated   = CURRENT_TIMESTAMP()
            WHERE user_id = @user_id
            """
            update_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
                    bigquery.ArrayQueryParameter("liked", "STRING", merged_liked),
                    bigquery.ArrayQueryParameter("disliked", "STRING", merged_disliked),
                    bigquery.ArrayQueryParameter("skipped", "STRING", merged_skipped),
                    bigquery.ArrayQueryParameter("replayed", "STRING", merged_replayed),
                ]
            )
            bq.query(update_query, job_config=update_config).result()
            logger.info(f"Updated preferences for user {user_id}")

        else:
            insert_query = f"""
            INSERT INTO `{table_id}`
                (user_id, liked_songs, disliked_songs, skipped_songs, replayed_songs, last_updated)
            VALUES
                (@user_id, @liked, @disliked, @skipped, @replayed, CURRENT_TIMESTAMP())
            """
            insert_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
                    bigquery.ArrayQueryParameter("liked", "STRING", liked),
                    bigquery.ArrayQueryParameter("disliked", "STRING", disliked),
                    bigquery.ArrayQueryParameter("skipped", "STRING", skipped),
                    bigquery.ArrayQueryParameter("replayed", "STRING", replayed),
                ]
            )
            bq.query(insert_query, job_config=insert_config).result()
            logger.info(f"Inserted preferences for new user {user_id}")

    except Exception as e:
        logger.error(f"Failed to merge preferences for user {user_id}: {e}")
