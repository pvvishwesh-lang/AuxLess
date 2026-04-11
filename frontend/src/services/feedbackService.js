import { db } from '../config/firebase';
import {
  doc, updateDoc, increment,
  collection, addDoc, serverTimestamp
} from 'firebase/firestore';

const CLOUD_RUN = process.env.REACT_APP_CLOUD_RUN_URL;

// ── Publish feedback event to Pub/Sub ───────────────────────
// Matches ParseEventFn.VALID_ACTIONS = {play,skip,complete,like,dislike,replay}
export const publishFeedback = async ({ roomId, songId, userId, action, songName, artist }) => {
  const event = {
    room_id:    roomId,
    song_id:    songId,
    user_id:    userId,
    event_type: action,        // ParseEventFn reads event_type or action
    action,
    song_name:  songName,
    artist,
    timestamp:  new Date().toISOString(),
    session_id: roomId,
  };

  // 1. Write to Firestore immediately (UpdateFirestoreFn shape)
  // sessions/{roomId}/user_feedback/{userId}_{songId}
  const feedbackRef = doc(
    db, 'sessions', roomId, 'user_feedback', `${userId}_${songId}`
  );
  await updateDoc(feedbackRef, {
    action,
    last_updated: serverTimestamp(),
  }).catch(() =>
    // doc doesn't exist yet — create it
    addDoc(collection(db, 'sessions', roomId, 'user_feedback'), {
      ...event,
      last_updated: serverTimestamp(),
    })
  );

  // 2. Update track score immediately in Firestore
  // Mirrors ScoreFeedbackFn weights: like+2, dislike-2, skip-0.5, replay+1.5
  const WEIGHTS = { like: 2, dislike: -2, skip: -0.5, replay: 1.5, play: 0, complete: 0.5 };
  const delta   = WEIGHTS[action] || 0;

  if (delta !== 0) {
    const trackRef = doc(db, 'sessions', roomId, 'tracks', songId);
    await updateDoc(trackRef, {
      score:              increment(delta),
      [`${action}_count`]:increment(1),
      last_updated:       serverTimestamp(),
    }).catch(() => {});
  }

  // 3. Also send to Cloud Run → Pub/Sub → Dataflow streaming pipeline
  if (CLOUD_RUN) {
    fetch(`${CLOUD_RUN}/feedback`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(event),
    }).catch(e => console.warn('Pub/Sub publish failed:', e));
  }
};