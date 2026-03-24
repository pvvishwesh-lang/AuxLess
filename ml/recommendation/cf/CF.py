"""
Collaborative Filtering (CF) recommendation module.

Algorithm: item-item co-occurrence with population-level liked-song data.

The core idea:
    "Across all users in our database, what songs do people tend to
     like alongside the songs this group already likes?"

For each candidate song, we find the single strongest connection between
that song and any of the group's liked songs: the maximum number of
population users who liked both the candidate and that particular seed song.
That peak co-occurrence count is then normalised by the candidate's total
popularity (how many users liked it at all) to avoid surfacing songs that
score high only because everyone has them.

The result is a per-song CF score in [0, 1]:
    1.0 = all users who liked the best-connected group seed also liked this song
    0.0 = no population users who liked any group seed also liked this song

Using max() over group seeds rather than sum() prevents a single population
user who liked the candidate and many group seeds from inflating the score
as if multiple independent users had co-occurred.

Fallback strategy:
    If the population data is empty (new deployment, cold database)
    or no group-liked songs appear in the co-occurrence index at all,
    CF falls back to embedding-based similarity between candidate songs
    and the group's liked-song embeddings.  This produces scores in
    the same [0, 1] range so _aggregate_scores() in main.py requires
    no special-casing.

Output contract (mirrors CBF.py and gru_inference.py):
    DataFrame with columns: video_id, track_title, artist_name, genre, cf_score
    Sorted by cf_score descending.  Length = min(top_n, available candidates).
    Empty DataFrame when songs_df is empty, all songs are played,
    or no group liked songs are available.
"""

import logging
from collections import defaultdict

import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

from ml.recommendation.config import TOP_N, CF_MIN_COOCCURRENCE

logger = logging.getLogger(__name__)


# ── Co-occurrence index builder ───────────────────────────────────────────────

def build_cooccurrence_index(all_user_liked: dict) -> tuple[dict, dict]:
    """
    Builds two lookup structures from the full population liked-song data.

    This function depends only on all_user_liked (the fixed population cache)
    and is independent of which group songs are present this session.
    The index is therefore safe to cache on SessionState after first build —
    see get_cf_scores() which stores it on state if a state object is passed.

    Args:
        all_user_liked: { user_id: [video_id, ...] }
                        Full population data from SessionState.all_user_liked.

    Returns:
        cooccurrence : { video_id_a: { video_id_b: count } }
            For every pair (a, b) that appeared in the same user's liked
            list, stores how many users liked both.

        popularity   : { video_id: count }
            How many users liked each song in the population.
            Used for normalisation.
    """
    cooccurrence: dict = defaultdict(lambda: defaultdict(int))
    popularity:   dict = defaultdict(int)

    for liked_ids in all_user_liked.values():
        # deduplicate per user so one user with duplicates in their
        # BigQuery array doesn't inflate either popularity or co-occurrence
        unique_ids = list(set(liked_ids))

        # update popularity — count each video_id at most once per user
        for vid in unique_ids:
            popularity[vid] += 1

        # update co-occurrence for every pair in this user's liked list
        for i, vid_a in enumerate(unique_ids):
            for vid_b in unique_ids[i + 1:]:
                cooccurrence[vid_a][vid_b] += 1
                cooccurrence[vid_b][vid_a] += 1

    logger.info(
        f"Co-occurrence index: {len(popularity)} songs, "
        f"{sum(len(v) for v in cooccurrence.values())} pairs"
    )
    return dict(cooccurrence), dict(popularity)


# ── Per-song CF scorer ────────────────────────────────────────────────────────

def _score_by_cooccurrence(
    candidate_video_ids: list,
    group_liked_ids:     set,
    cooccurrence:        dict,
    popularity:          dict,
    min_cooccurrence:    int,
) -> dict:
    """
    Scores each candidate song by its co-occurrence with the group's
    liked songs, normalised by the song's total popularity.

    Score formula for candidate song C:
        raw_signal  = max co-occurrence count between C and any group-liked song G
        norm_factor = popularity[C]  (how many users liked C at all)
        cf_score    = raw_signal / norm_factor   (clipped to [0, 1])

    Using max() instead of sum() over group-liked seeds avoids multi-seed
    double-counting: a single population user who liked the candidate AND
    multiple group-liked seeds would otherwise be counted once per seed,
    inflating raw_signal beyond the number of distinct users who co-occurred.
    max() reads: "what is the strongest single-seed connection to this candidate?"
    which is both semantically correct and discriminates better between candidates
    that score 1.0 under sum (because the clip masks the distortion).

    The normalisation prevents "everybody likes this" songs from dominating.
    A song that 1,000 users liked, and 900 of those also liked a group song,
    scores higher (0.9) than a song that 10,000 users liked where only 500
    overlapped with the group (0.05).

    Pairs with fewer than min_cooccurrence occurrences are ignored as noise.

    Args:
        candidate_video_ids: list of video_ids to score
        group_liked_ids:     set of video_ids liked by any group member
        cooccurrence:        output of build_cooccurrence_index()
        popularity:          output of build_cooccurrence_index()
        min_cooccurrence:    minimum overlap count to trust (from config)

    Returns:
        { video_id: cf_score_float }  — scores in [0, 1], default 0.0
    """
    scores: dict = {}

    for candidate in candidate_video_ids:
        candidate_cooc = cooccurrence.get(candidate, {})
        if not candidate_cooc:
            scores[candidate] = 0.0
            continue

        # take the MAX co-occurrence count across all group-liked seeds.
        # sum() would double-count a population user who liked the candidate
        # alongside multiple group seeds — max() correctly attributes the
        # signal to the single strongest seed connection.
        raw_signal = max(
            (count
             for group_vid, count in candidate_cooc.items()
             if group_vid in group_liked_ids and count >= min_cooccurrence),
            default=0,
        )

        if raw_signal == 0:
            scores[candidate] = 0.0
            continue

        # normalise by total popularity — clip to [0, 1]
        pop = popularity.get(candidate, 1)
        scores[candidate] = min(raw_signal / max(pop, 1), 1.0)

    return scores


# ── Embedding fallback ────────────────────────────────────────────────────────

def _score_by_embedding_fallback(
    candidate_songs:  pd.DataFrame,
    group_liked_ids:  set,
    songs_df:         pd.DataFrame,
) -> dict:
    """
    Fallback when co-occurrence data is unavailable or produces all zeros.

    Computes cosine similarity between each candidate song's embedding and
    the mean embedding of the group's liked songs.  Produces scores in
    [0, 1] that are compatible with co-occurrence scores in the aggregation
    step.

    This is intentionally lightweight — CBF already does a richer version
    of this computation.  The fallback's purpose is to keep CF contributing
    a non-zero signal rather than silently being absent.

    Note: this fallback is not truly "collaborative" — it doesn't use any
    population behaviour.  It is a robustness measure for cold deployments.
    If your product prefers CF to return empty rather than an embedding-based
    approximation, return an empty dict here instead of computing similarity.

    Returns:
        { video_id: cf_score_float }
    """
    liked_songs = songs_df[songs_df["video_id"].isin(group_liked_ids)]
    if liked_songs.empty:
        return {vid: 0.0 for vid in candidate_songs["video_id"]}

    liked_matrix    = np.stack(liked_songs["embedding"].values)
    mean_liked      = liked_matrix.mean(axis=0, keepdims=True)          # (1, 386)

    candidate_matrix = np.stack(candidate_songs["embedding"].values)    # (N, 386)
    sims             = cosine_similarity(mean_liked, candidate_matrix)[0]

    # cosine similarity is in [-1, 1]; rescale to [0, 1]
    sims_scaled = (sims + 1.0) / 2.0

    return dict(zip(candidate_songs["video_id"], sims_scaled.tolist()))


# ── Public API ────────────────────────────────────────────────────────────────

def get_cf_scores(
    all_user_liked:     dict,
    user_liked_songs:   dict,
    songs_df:           pd.DataFrame,
    already_played_ids: set,
    top_n:              int = TOP_N,
    min_cooccurrence:   int = CF_MIN_COOCCURRENCE,
    state:              object = None,
) -> pd.DataFrame:
    """
    Collaborative Filtering recommendation.

    Args:
        all_user_liked:     { user_id: [video_id, ...] }
                            Full population data from SessionState.all_user_liked.
                            Fetched once at session init, passed in each cycle.

        user_liked_songs:   { user_id: [video_id, ...] }
                            Only the current session users' liked songs.
                            Used to define the group's taste profile.

        songs_df:           Full song catalog DataFrame from BigQuery.
                            Must have columns: video_id, track_title,
                            artist_name, genre, embedding.

        already_played_ids: set of video_ids played this session.
                            Excluded from candidates.

        top_n:              Number of top-scoring songs to return.

        min_cooccurrence:   Minimum overlap count to trust (from config).

        state:              Optional SessionState object.  When provided,
                            the co-occurrence index is built once and cached
                            on state._cf_cooccurrence / state._cf_popularity
                            for all subsequent recommendation cycles.
                            Large CPU win when CF_MAX_USERS is high.

    Returns:
        DataFrame with columns: video_id, track_title, artist_name,
                                 genre, cf_score
        Sorted by cf_score descending, length = min(top_n, candidates).
        Empty DataFrame if no candidates remain after exclusion.

    Degradation path:
        all_user_liked empty  →  embedding fallback
        no co-occurrence hits →  embedding fallback
        songs_df empty        →  empty DataFrame
    """
    if songs_df.empty:
        logger.warning("CF: empty songs_df — returning empty.")
        return pd.DataFrame()

    # ── 1. build candidate pool ───────────────────────────────────────────────
    # exclude songs already played this session from CF candidates,
    # but do NOT exclude liked/disliked songs here — that is CBF's job.
    # CF's signal is "population says these fit" not "group hasn't heard these".
    candidate_songs = songs_df[
        ~songs_df["video_id"].isin(already_played_ids)
    ].reset_index(drop=True)

    if candidate_songs.empty:
        logger.warning("CF: no candidate songs after exclusion.")
        return pd.DataFrame()

    # ── 2. collect group-liked video_ids ─────────────────────────────────────
    group_liked_ids: set = {
        vid
        for liked_ids in user_liked_songs.values()
        for vid in liked_ids
    }

    if not group_liked_ids:
        logger.warning(
            "CF: no group liked songs available. "
            "CF will return empty — CBF/GRU handle this case."
        )
        return pd.DataFrame()

    logger.info(
        f"CF: {len(candidate_songs)} candidates, "
        f"{len(group_liked_ids)} group-liked songs, "
        f"{len(all_user_liked)} population users"
    )

    # ── 3. score candidates ───────────────────────────────────────────────────
    use_fallback = False

    if not all_user_liked:
        logger.warning("CF: no population data — using embedding fallback.")
        use_fallback = True
    else:
        # use cached index if available on state, otherwise build and cache it
        if state is not None and hasattr(state, "_cf_cooccurrence"):
            cooccurrence = state._cf_cooccurrence
            popularity   = state._cf_popularity
            logger.debug("CF: using cached co-occurrence index.")
        else:
            cooccurrence, popularity = build_cooccurrence_index(all_user_liked)
            if state is not None:
                state._cf_cooccurrence = cooccurrence
                state._cf_popularity   = popularity
                logger.info("CF: co-occurrence index built and cached on SessionState.")

        candidate_ids = candidate_songs["video_id"].tolist()
        raw_scores    = _score_by_cooccurrence(
            candidate_ids, group_liked_ids,
            cooccurrence, popularity, min_cooccurrence,
        )

        # if the group's liked songs don't appear in the index at all,
        # fall back to embeddings rather than returning all-zeros
        if all(s == 0.0 for s in raw_scores.values()):
            logger.warning(
                "CF: co-occurrence produced all zeros "
                "(group songs not in population index). "
                "Falling back to embedding similarity."
            )
            use_fallback = True

    if use_fallback:
        raw_scores = _score_by_embedding_fallback(
            candidate_songs, group_liked_ids, songs_df
        )

    # ── 4. attach scores and rank ─────────────────────────────────────────────
    candidate_songs = candidate_songs.copy()
    candidate_songs["cf_score"] = candidate_songs["video_id"].map(
        lambda vid: raw_scores.get(vid, 0.0)
    )

    if top_n < 1:
        logger.warning(f"CF called with top_n={top_n}; returning empty.")
        return pd.DataFrame()

    recommendations = (
        candidate_songs
        .sort_values("cf_score", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    if recommendations.empty:
        logger.warning("CF: no recommendations after scoring.")
        return pd.DataFrame()

    top = recommendations.iloc[0]
    logger.info(
        f"CF top {top_n}: '{top['track_title']}' "
        f"(cf_score={top['cf_score']:.4f}, "
        f"{'co-occurrence' if not use_fallback else 'embedding fallback'})"
    )

    return recommendations[["video_id", "track_title", "artist_name", "genre", "cf_score"]]
