// pipelineService.js
// All pipeline + ML via Firestore only
import { mlDb } from '../config/firebase';
import {
  collection, doc,
  addDoc, updateDoc, setDoc,
  increment, serverTimestamp,
} from 'firebase/firestore';

const toSid = (roomId) => (roomId || '').replace(/^AUX-/i, '').toLowerCase();

export async function publishMusicEvent({ roomId, userId, songId, eventType, songName, artist }) {
  if (!roomId || !songId) return;
  const sid = toSid(roomId);
  try {
    await addDoc(collection(mlDb, 'sessions', sid, 'feedback_events'), {
      room_id:    roomId,
      user_id:    userId    || 'guest',
      song_id:    songId,
      event_type: eventType,
      song_name:  songName  || 'Unknown',
      artist:     artist    || 'Unknown',
      timestamp:  serverTimestamp(),
    });
    console.log(`[Pipeline] ✅ Firestore event — type=${eventType} song=${songName}`);
  } catch (e) {
    console.warn('[Pipeline] feedback_event write error:', e.message);
  }
}

export async function writeSongEvent({ roomId, song, playOrder, sessionNum }) {
  if (!roomId || !song) return null;
  const sid = toSid(roomId);
  try {
    const ref = await addDoc(collection(mlDb, 'sessions', sid, 'song_events'), {
      video_id:       song?.id    || song?.video_id || '',
      song_name:      song?.title || song?.song_name || '',
      artist:         song?.artist || '',
      genre:          song?.genre  || '',
      play_order:     playOrder,
      liked_flag:     0,
      timestamp:      serverTimestamp(),
      session_number: sessionNum,
    });
    console.log(`[Pipeline] ✅ song_event written — ${song?.title} order=${playOrder}`);
    return ref.id;
  } catch (e) {
    console.warn('[Pipeline] song_event write error:', e.message);
    return null;
  }
}

export async function updateLikedFlag({ roomId, songEventId }) {
  if (!roomId || !songEventId) return;
  const sid = toSid(roomId);
  try {
    await updateDoc(doc(mlDb, 'sessions', sid, 'song_events', songEventId), { liked_flag: 1 });
    console.log(`[ML] ✅ liked_flag updated`);
  } catch (e) {
    console.warn('[ML] liked_flag update error:', e.message);
  }
}

export async function incrementSongsPlayed({ roomId, playOrder }) {
  if (!roomId) return;
  const sid = toSid(roomId);
  try {
    await updateDoc(doc(mlDb, 'sessions', sid, 'metadata', 'state'), {
      songs_played_count: increment(1),
      current_play_order: playOrder,
    });
  } catch (e) {
    try {
      await setDoc(doc(mlDb, 'sessions', sid, 'metadata', 'state'), {
        songs_played_count: 1,
        session_number:     1,
        current_session_id: sid,
        current_play_order: playOrder,
        status:             'active',
        createdAt:          Date.now(),
      });
    } catch (e2) {
      console.warn('[Pipeline] incrementSongsPlayed error:', e2.message);
    }
  }
}

export async function endSession({ roomId, sessionNum }) {
  if (!roomId) return sessionNum + 1;
  const sid = toSid(roomId);
  try {
    await updateDoc(doc(mlDb, 'sessions', sid, 'metadata', 'state'), {
      status:   'ended',
      ended_at: serverTimestamp(),
    });
    console.log('[ML] ✅ Session ended — ML will generate recommendations');

    const newNum = sessionNum + 1;
    setTimeout(async () => {
      try {
        await updateDoc(doc(mlDb, 'sessions', sid, 'metadata', 'state'), {
          status:         'active',
          session_number: newNum,
          started_at:     serverTimestamp(),
          // songs_played_count removed — don't reset mid-session!
        });
        console.log(`[ML] ✅ New session #${newNum} started`);
      } catch (e) {
        console.warn('[ML] New session start failed:', e.message);
      }
    }, 3000);

    return newNum;
  } catch (e) {
    console.warn('[ML] endSession error:', e.message);
    return sessionNum + 1;
  }
}