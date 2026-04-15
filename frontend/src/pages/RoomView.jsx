import React, { useState, useEffect, useRef, useCallback } from 'react';
import toast from 'react-hot-toast';
import { T } from '../styles/tokens';
import { MOCK_MEMBERS, GENRES_DIST, MOCK_FEEDBACK_SCORES } from '../data/mockData';
import Waveform    from '../components/Waveform';
import Avatar      from '../components/Avatar';
import GBar        from '../components/GBar';
import Blobs       from '../components/Blobs';
import Btn         from '../components/Btn';
import ArtistImage from '../components/ArtistImage';
import {
  publishMusicEvent,
  writeSongEvent,
  updateLikedFlag,
  incrementSongsPlayed,
  endSession as endSessionFirestore,
} from '../services/pipelineService';

import { db, mlDb } from '../config/firebase';
import {
  collection, doc, onSnapshot,
  updateDoc, increment, addDoc,
  serverTimestamp, getDoc,
} from 'firebase/firestore';

const WEIGHTS           = { like: 2, dislike: -2, skip: -0.5, replay: 1.5, play: 0, complete: 0.5 };
const SONGS_PER_SESSION = 10;
const toSessionId       = (id) => (id || '').replace(/^AUX-/i, '').toLowerCase();

function YouTubePlayer({ videoId, startedAt, onEnd }) {
  const [ready,   setReady]   = useState(false);
  const [clicked, setClicked] = useState(false);
  const playerRef = useRef(null);

  useEffect(() => {
    if (!videoId) return;
    const initPlayer = () => {
      try { playerRef.current?.destroy(); } catch {}
      playerRef.current = new window.YT.Player('yt-player', {
        videoId,
        width: '1', height: '1',
        playerVars: { autoplay: 1, controls: 0, rel: 0, playsinline: 1 },
        events: {
          onReady: (e) => {
            if (startedAt) {
              const elapsed = Math.floor((Date.now() - startedAt) / 1000);
              if (elapsed > 0) e.target.seekTo(elapsed, true);
            }
            setReady(true);
          },
          onStateChange: (e) => {
            if (e.data === window.YT.PlayerState.ENDED) onEnd();
          },
        },
      });
    };
    if (window.YT && window.YT.Player) {
      initPlayer();
    } else {
      if (!document.querySelector('script[src="https://www.youtube.com/iframe_api"]')) {
        const tag = document.createElement('script');
        tag.src   = 'https://www.youtube.com/iframe_api';
        document.head.appendChild(tag);
      }
      window.onYouTubeIframeAPIReady = initPlayer;
    }
    return () => { try { playerRef.current?.destroy(); } catch {} };
  }, [videoId, startedAt, onEnd]);

  const handleClick = () => {
    setClicked(true);
    try { playerRef.current?.playVideo(); } catch {}
  };

  return (
    <>
      <div style={{ position:'fixed', top:'-9999px', left:'-9999px', width:1, height:1 }}>
        <div id="yt-player" />
      </div>
      {ready && !clicked && (
        <div onClick={handleClick}
          style={{ position:'fixed', bottom:80, right:20, zIndex:200,
            background:T.green, borderRadius:999, padding:'8px 16px',
            cursor:'pointer', boxShadow:'0 2px 12px rgba(29,185,84,0.35)',
            display:'flex', alignItems:'center', gap:6 }}>
          <span style={{ fontSize:14 }}>▶</span>
          <span style={{ fontSize:12, fontWeight:700, color:'#060610' }}>Start music</span>
        </div>
      )}
    </>
  );
}

function ShareModal({ roomId, onClose }) {
  const link = `https://aux-less.vercel.app`;
  const copy = (text) => { navigator.clipboard?.writeText(text).catch(() => {}); toast.success('Copied!'); };
  return (
    <div style={{ position:'fixed', inset:0, zIndex:200, background:'rgba(0,0,0,0.75)',
      backdropFilter:'blur(8px)', display:'flex', alignItems:'center', justifyContent:'center', padding:16 }}
      onClick={onClose}>
      <div className="fade-up" onClick={e => e.stopPropagation()}
        style={{ background:T.card, borderRadius:22, border:`1px solid ${T.border}`, padding:'28px 24px', width:'100%', maxWidth:400 }}>
        <div style={{ textAlign:'center', marginBottom:22 }}>
          <div style={{ fontSize:36, marginBottom:10 }}>🎉</div>
          <h3 style={{ fontSize:20, fontWeight:800, color:T.text, marginBottom:6 }}>Invite your friends</h3>
          <p style={{ fontSize:13, color:T.muted }}>Share the room code or link</p>
        </div>
        <div style={{ marginBottom:12 }}>
          <div style={{ fontSize:11, fontWeight:700, color:T.muted, textTransform:'uppercase', letterSpacing:'.08em', marginBottom:8 }}>Room code</div>
          <div style={{ display:'flex', alignItems:'center', gap:10, background:T.surface, borderRadius:12, padding:'12px 16px', border:`1px solid ${T.green}44` }}>
            <span style={{ flex:1, fontFamily:'monospace', fontSize:22, fontWeight:800, letterSpacing:'0.15em', color:T.green }}>{roomId}</span>
            <button onClick={() => copy(roomId)} style={{ padding:'6px 14px', borderRadius:8, border:`1px solid ${T.green}44`, background:T.greenLo, color:T.green, fontSize:12, fontWeight:700, cursor:'pointer' }}>Copy</button>
          </div>
        </div>
        <div style={{ marginBottom:20 }}>
          <div style={{ fontSize:11, fontWeight:700, color:T.muted, textTransform:'uppercase', letterSpacing:'.08em', marginBottom:8 }}>Invite link</div>
          <div style={{ display:'flex', alignItems:'center', gap:10, background:T.surface, borderRadius:12, padding:'10px 14px', border:`1px solid ${T.border}` }}>
            <span style={{ flex:1, fontSize:12, color:T.muted, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{link}</span>
            <button onClick={() => copy(link)} style={{ padding:'6px 14px', borderRadius:8, border:`1px solid ${T.border}`, background:'rgba(255,255,255,0.06)', color:T.text, fontSize:12, fontWeight:700, cursor:'pointer', flexShrink:0 }}>Copy</button>
          </div>
        </div>
        <div style={{ display:'flex', gap:8, marginBottom:16 }}>
          {[{ icon:'💬', label:'WhatsApp', color:'#25D366' }, { icon:'✈️', label:'Telegram', color:'#2AABEE' }, { icon:'📱', label:'Messages', color:'#34C759' }].map(p => (
            <button key={p.label} onClick={() => toast.success(`Opening ${p.label}…`)}
              style={{ flex:1, padding:'10px 4px', borderRadius:10, background:`${p.color}18`,
                border:`1px solid ${p.color}33`, color:p.color, fontSize:12, fontWeight:600,
                cursor:'pointer', display:'flex', flexDirection:'column', alignItems:'center', gap:4 }}>
              <span style={{ fontSize:18 }}>{p.icon}</span>{p.label}
            </button>
          ))}
        </div>
        <Btn variant="outline" onClick={onClose} full>Done</Btn>
      </div>
    </div>
  );
}

export default function RoomView({ roomId, user, onLeave }) {
  const code          = roomId || 'AUX-7749';
  const sessionId     = toSessionId(code);
  const queueCacheKey = `queue_${sessionId}`;
  const playedKey     = `played_${sessionId}`;

  const [queue,              setQueue]              = useState([]);
  const [members,            setMembers]            = useState(MOCK_MEMBERS);
  const [genreDist]                                 = useState(GENRES_DIST);
  const [scores,             setScores]             = useState(MOCK_FEEDBACK_SCORES);
  const [liveEvents,         setLiveEvents]         = useState(0);
  const [rtab,               setRtab]               = useState('queue');
  const [showShare,          setShowShare]          = useState(false);
  const [isLive,             setIsLive]             = useState(false);
  const [hostUid,            setHostUid]            = useState(null);
  const [songsPlayed,        setSongsPlayed]        = useState(0);
  const [sessionNum,         setSessionNum]         = useState(1);
  const [playOrder,          setPlayOrder]          = useState(1);
  const [currentSongEventId, setCurrentSongEventId] = useState(null);
  const [nowPlaying,         setNowPlaying]         = useState(null);
  const [sessionStarted,     setSessionStarted]     = useState(false);

  const playOrderRef   = useRef(1);
  const queueRef       = useRef([]);
  const sessionNumRef  = useRef(1);
  const songsPlayedRef = useRef(0);
  const songEventIdRef = useRef(null);
  const startedRef     = useRef(false);
  const isFirstLoad    = useRef(true);

  useEffect(() => { playOrderRef.current   = playOrder;          }, [playOrder]);
  useEffect(() => { queueRef.current       = queue;              }, [queue]);
  useEffect(() => { sessionNumRef.current  = sessionNum;         }, [sessionNum]);
  useEffect(() => { songsPlayedRef.current = songsPlayed;        }, [songsPlayed]);
  useEffect(() => { songEventIdRef.current = currentSongEventId; }, [currentSongEventId]);

  const isHost = hostUid ? hostUid === user?.uid : false;

  useEffect(() => {
    if (roomId) sessionStorage.setItem('auxless_room', roomId);
  }, [roomId]);

  // ── Helper: call Cloud Run end_session ────────────────────
  const callEndSession = (sid) => {
    const url = process.env.REACT_APP_CLOUD_FUNCTION_URL;
    if (url && sid) {
      fetch(`${url}/end_session`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ session_id: sid }),
      }).catch(() => {});
    }
  };

  // ── Restore cached queue on mount ────────────────────────
  useEffect(() => {
    try {
      const cached = JSON.parse(localStorage.getItem(queueCacheKey) || '[]');
      if (cached.length > 0) {
        setQueue(cached);
        setIsLive(true);
        startedRef.current = true;
        console.log('[Queue] Restored', cached.length, 'songs from cache');
      }
    } catch {}
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ── Song start ──────────────────────────────────────────────
  const handleSongStart = useCallback(async (song, order) => {
    if (!song) return;

    const played = JSON.parse(localStorage.getItem(playedKey) || '[]');
    if (!played.includes(song.id)) {
      played.push(song.id);
      localStorage.setItem(playedKey, JSON.stringify(played));
    }

    try {
      const cached  = JSON.parse(localStorage.getItem(queueCacheKey) || '[]');
      const updated = cached.filter(s => s.id !== song.id);
      localStorage.setItem(queueCacheKey, JSON.stringify(updated));
    } catch {}

    setNowPlaying(song);
    if (roomId) {
      try {
        await updateDoc(doc(db, 'rooms', roomId), {
          now_playing: {
            video_id:   song.video_id || song.id || '',
            title:      song.title    || '',
            artist:     song.artist   || '',
            genre:      song.genre    || '',
            score:      song.score    || 0,
            started_at: Date.now(),
          }
        });
      } catch {}
    }
    try {
      const eventId = await writeSongEvent({ roomId, song, playOrder: order, sessionNum: sessionNumRef.current });
      setCurrentSongEventId(eventId);
      await publishMusicEvent({
        roomId, userId: user?.uid || 'guest',
        songId: song?.video_id || song?.id || '',
        eventType: 'play', songName: song?.title || '', artist: song?.artist || '',
      });
      await incrementSongsPlayed({ roomId, playOrder: order });
      const newCount = songsPlayedRef.current + 1;
      setSongsPlayed(newCount);
      if (newCount > 0 && newCount % SONGS_PER_SESSION === 0) {
        const newNum = await endSessionFirestore({ roomId, sessionNum: sessionNumRef.current });
        setSessionNum(newNum);
        sessionNumRef.current = newNum;
      }
    } catch (e) {
      console.warn('handleSongStart failed:', e);
    }
  }, [roomId, user, sessionId, playedKey, queueCacheKey]);

  // ── Song ends → play highest liked next ──────────────────
  const handleSongEnd = useCallback(() => {
    const current = queueRef.current;
    if (!current || current.length < 2) {
      startedRef.current = false;
      setIsLive(false);
      return;
    }
    const playing = current.find(t => t.playing) || current[0];

    const played = JSON.parse(localStorage.getItem(playedKey) || '[]');
    if (!played.includes(playing.id)) {
      played.push(playing.id);
      localStorage.setItem(playedKey, JSON.stringify(played));
    }

    try {
      const cached  = JSON.parse(localStorage.getItem(queueCacheKey) || '[]');
      const updated = cached.filter(s => s.id !== playing.id);
      localStorage.setItem(queueCacheKey, JSON.stringify(updated));
    } catch {}

    publishMusicEvent({
      roomId, userId: user?.uid || 'guest',
      songId: playing?.video_id || playing?.id || '',
      eventType: 'complete', songName: playing?.title || '', artist: playing?.artist || '',
    });

    const nextOrder = playOrderRef.current + 1;
    setPlayOrder(nextOrder);
    playOrderRef.current = nextOrder;

    const rest     = current.filter(t => t.id !== playing?.id).sort((a, b) => b.score - a.score);
    const nextSong = rest[0];
    if (!nextSong) {
      startedRef.current = false;
      setIsLive(false);
      return;
    }
    const newQueue = rest.map((t, i) => ({ ...t, playing: i === 0 }));
    setQueue(newQueue);
    handleSongStart(nextSong, nextOrder);
  }, [roomId, user, handleSongStart, playedKey, queueCacheKey]);

  // ── ML recommendations listener ───────────────────────────
  useEffect(() => {
    if (!roomId) return;

    const unsub = onSnapshot(
      collection(mlDb, 'sessions', sessionId, 'recommendations'),
      (snap) => {
        const played = JSON.parse(localStorage.getItem(playedKey) || '[]');

        if (snap.empty) {
          console.log('[Queue] Firestore empty — keeping cached queue intact');
          return;
        }

        const allDocs = snap.docs.map(d => d.id);
        const allNew  = allDocs.every(id => !played.includes(id));

        if (allNew && allDocs.length > 5 && played.length > 0) {
          console.log('[Queue] New batch detected! Clearing cache for fresh start');
          localStorage.removeItem(playedKey);
          localStorage.removeItem(queueCacheKey);
          startedRef.current = false;
          setIsLive(false);
        }

        const playedNow = JSON.parse(localStorage.getItem(playedKey) || '[]');

        const cachedNow = (() => {
          try { return JSON.parse(localStorage.getItem(queueCacheKey) || '[]'); } catch { return []; }
        })();

        const firestoreSongs = snap.docs.map((d) => {
          const existing = queueRef.current.find(q => q.id === d.id)
                        || cachedNow.find(q => q.id === d.id);
          return {
            id:       d.id,
            title:    d.data().track_title  || d.data().song_name || d.data().title || 'Unknown',
            artist:   d.data().artist_name  || d.data().artist    || '',
            genre:    d.data().genre        || '',
            likes:    d.data().like_count   || (existing?.likes    ?? 0),
            dislikes: d.data().dislike_count || (existing?.dislikes ?? 0),
            score:    d.data().final_score  || (existing?.score     ?? 0),
            playing:  existing?.playing     ?? false,
            video_id: d.data().video_id     || d.id,
            image:    d.data().artwork_url  || d.data().image_url  || null,
          };
        });

        const firestoreIds   = new Set(firestoreSongs.map(s => s.id));
        const survivingSongs = cachedNow.filter(
          s => !firestoreIds.has(s.id) && !playedNow.includes(s.id)
        );

        const allSongs = [...firestoreSongs, ...survivingSongs]
          .filter(s => !playedNow.includes(s.id));

        if (allSongs.length === 0) return;

        allSongs.sort((a, b) => b.score - a.score);

        const currentPlaying = queueRef.current.find(t => t.playing);
        if (currentPlaying) {
          const idx = allSongs.findIndex(r => r.id === currentPlaying.id);
          if (idx > 0) {
            const [p] = allSongs.splice(idx, 1);
            p.playing = true;
            allSongs.unshift(p);
          } else if (idx === 0) {
            allSongs[0].playing = true;
          }
        } else if (allSongs.length > 0) {
          allSongs[0].playing = true;
        }

        localStorage.setItem(queueCacheKey, JSON.stringify(allSongs));

        setQueue(allSongs);
        setIsLive(true);
        isFirstLoad.current = false;
      },
      () => {}
    );
    return () => unsub();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [roomId]);

  // ── Start song when ML arrives or new batch arrives ───────
  useEffect(() => {
    if (startedRef.current || !isLive || queue.length === 0) return;
    startedRef.current = true;
    const first = queue.find(t => t.playing) || queue[0];
    handleSongStart(first, playOrderRef.current);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isLive, queue]);

  // ── Tracks scores listener ────────────────────────────────
  useEffect(() => {
    if (!roomId) return;
    const unsub = onSnapshot(collection(mlDb, 'sessions', sessionId, 'tracks'), (snap) => {
      if (snap.empty) return;
      setScores(snap.docs.map(d => ({
        video_id:  d.id,
        song_name: d.data().song_name || d.id,
        score:     d.data().score     || 0,
      })));
      setLiveEvents(prev => prev + snap.docChanges().length);
    }, () => {});
    return () => unsub();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [roomId]);

  // ── Room members + host + NOW PLAYING SYNC for ALL users ──
  useEffect(() => {
    if (!roomId) return;
    const unsub = onSnapshot(doc(db, 'rooms', roomId), (snap) => {
      if (!snap.exists()) return;
      const data = snap.data();
      setHostUid(data.host_uid || null);
      if (data.status === 'ended') {
        sessionStorage.removeItem('auxless_room');
        onLeave();
        return;
      }

      const np = data.now_playing;
      if (np?.video_id) {
        setIsLive(true);
        startedRef.current = true;
        setSessionStarted(true);

        setNowPlaying(prev => {
          if (prev?.video_id !== np.video_id) {
            setQueue(q => {
              if (!q.length) return q;
              const updated = q.map(t => ({
                ...t,
                playing: t.video_id === np.video_id || t.id === np.video_id,
              }));
              const playing = updated.find(t => t.playing);
              const rest    = updated.filter(t => !t.playing).sort((a, b) => b.score - a.score);
              return playing ? [playing, ...rest] : updated;
            });
            return {
              id:         np.video_id,
              video_id:   np.video_id,
              title:      np.title  || '',
              artist:     np.artist || '',
              genre:      np.genre  || '',
              score:      np.score  || 0,
              playing:    true,
              started_at: np.started_at,
            };
          }
          return prev;
        });
      }

      const users = data.users || [];
      if (!users.length) return;
      setMembers(users.filter(u => u.isactive !== false).map((u, i) => ({
        id: i, uid: u.uid,
        name:   u.name   || `User ${i + 1}`,
        av:     u.av     || (u.name || 'U').slice(0, 2).toUpperCase(),
        active: u.isactive !== false,
        isHost: u.isHost || false,
      })));
    }, () => {});
    return () => unsub();
  }, [roomId, onLeave]);

  // ── Host start session ────────────────────────────────────
  const handleStartSession = async () => {
    if (!isHost) return;
    try {
      await updateDoc(doc(mlDb, 'sessions', sessionId), { status: 'pending' });
      const url = process.env.REACT_APP_CLOUD_FUNCTION_URL;
      if (url) {
        fetch(`${url}/start_session`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ session_id: sessionId }),
        }).catch(() => {});
      }
      setSessionStarted(true);
      toast.success('Session started! ML is fetching all playlists… 🚀');
    } catch (e) {
      console.warn('Start session failed:', e);
      toast.error('Failed to start session');
    }
  };

  // ── Vote — one vote per user per song ─────────────────────
  const vote = async (id, type) => {
    const action = type === 'likes' ? 'like' : 'dislike';
    const delta  = WEIGHTS[action];
    const track  = queue.find(t => t.id === id);
    const uid    = user?.uid || 'guest';
    const voteKey = `vote_${sessionId}_${uid}_${id}`;
    const alreadyVoted = localStorage.getItem(voteKey);
    if (alreadyVoted) {
      toast.error('You already voted on this song!', { duration: 1500 });
      return;
    }
    localStorage.setItem(voteKey, action);
    setQueue(q => {
      const updated = q.map(t =>
        t.id === id ? { ...t, [type]: t[type] + 1, score: +(t.score + delta).toFixed(1) } : t
      );
      const playing = updated.find(t => t.playing);
      const rest    = updated.filter(t => !t.playing).sort((a, b) => b.score - a.score);
      return playing ? [playing, ...rest] : updated.sort((a, b) => b.score - a.score);
    });
    try {
      const recRef = doc(mlDb, 'sessions', sessionId, 'recommendations', id);
      await updateDoc(recRef, {
        like_count:    action === 'like'    ? increment(1) : increment(0),
        dislike_count: action === 'dislike' ? increment(1) : increment(0),
        final_score:   increment(delta),
      }).catch(() => {});
      await publishMusicEvent({
        roomId, userId: uid, songId: id,
        eventType: action, songName: track?.title || '', artist: track?.artist || '',
      });
      if (action === 'like' && songEventIdRef.current) {
        await updateLikedFlag({ roomId, songEventId: songEventIdRef.current });
      }
      const fbRef = doc(mlDb, 'sessions', sessionId, 'user_feedback', `${uid}_${id}`);
      await updateDoc(fbRef, {
        action, score_delta: delta, last_updated: serverTimestamp(), user_id: uid, video_id: id,
      }).catch(async () => {
        await addDoc(collection(mlDb, 'sessions', sessionId, 'user_feedback'), {
          user_id: uid, video_id: id, action, score_delta: delta, last_updated: serverTimestamp(),
        });
      });
    } catch (e) {
      console.warn('Vote failed:', e);
    }
  };

  // ── Host end room ─────────────────────────────────────────
  const handleEndRoom = async () => {
    if (!isHost) return;
    try {
      // Call Cloud Run end_session
      callEndSession(sessionId);

      await updateDoc(doc(mlDb, 'sessions', sessionId), {
        status: 'ended', ended_at: serverTimestamp(),
      }).catch(() => {});
      await updateDoc(doc(db, 'rooms', roomId), {
        status: 'ended', ended_at: serverTimestamp(),
      });
      toast.success('Room ended!');
    } catch (e) {
      console.warn('End room failed:', e);
    } finally {
      sessionStorage.removeItem('auxless_room');
      onLeave();
    }
  };

  // ── Leave ─────────────────────────────────────────────────
  const handleLeave = async () => {
    if (roomId && user?.uid) {
      try {
        const snap = await getDoc(doc(db, 'rooms', roomId));
        if (snap.exists()) {
          if (isHost) {
            // Call Cloud Run end_session when host leaves
            callEndSession(sessionId);

            await updateDoc(doc(db, 'rooms', roomId), {
              status: 'ended', ended_at: serverTimestamp(),
            });
            await updateDoc(doc(mlDb, 'sessions', sessionId), {
              status: 'ended', ended_at: serverTimestamp(),
            }).catch(() => {});
          } else {
            const updated = (snap.data().users || []).map(u =>
              u.uid === user.uid ? { ...u, isactive: false } : u
            );
            await updateDoc(doc(db, 'rooms', roomId), { users: updated });
          }
        }
      } catch {}
    }
    sessionStorage.removeItem('auxless_room');
    onLeave();
  };

  const now = nowPlaying || queue.find(t => t.playing) || queue[0];

  return (
    <div style={{ minHeight:'100vh', background:T.bg, color:T.text, fontFamily:'system-ui,sans-serif' }}>
      <Blobs />
      {showShare && <ShareModal roomId={code} onClose={() => setShowShare(false)} />}
      <YouTubePlayer videoId={isLive ? now?.video_id : null} startedAt={now?.started_at} onEnd={handleSongEnd} />

      <nav style={{ position:'sticky', top:0, zIndex:50, display:'flex', alignItems:'center',
        justifyContent:'space-between', padding:'0 20px', height:58,
        borderBottom:`1px solid ${T.border}`, backdropFilter:'blur(18px)', background:'rgba(8,8,15,.88)' }}>
        <div style={{ display:'flex', alignItems:'center', gap:9 }}>
          <div style={{ width:28, height:28, borderRadius:8, background:`linear-gradient(135deg,${T.green},${T.purple})`,
            display:'flex', alignItems:'center', justifyContent:'center', fontSize:14 }}>♫</div>
          <span style={{ fontWeight:900, fontSize:15, letterSpacing:'-0.4px' }}>AuxLess</span>
          {isHost && <span style={{ padding:'2px 8px', borderRadius:999, background:T.purpleLo,
            border:`1px solid ${T.purple}44`, fontSize:10, fontWeight:700, color:T.purple }}>👑 HOST</span>}
        </div>
        <div style={{ display:'flex', alignItems:'center', gap:8 }}>
          {isLive && (
            <div style={{ display:'flex', alignItems:'center', gap:5, padding:'3px 10px',
              borderRadius:999, background:T.greenLo, border:`1px solid ${T.green}44`,
              fontSize:10, fontWeight:700, color:T.green }}>
              <span style={{ width:5, height:5, borderRadius:'50%', background:T.green, animation:'pulse 2s infinite' }} />LIVE
            </div>
          )}
          <div style={{ padding:'5px 14px', background:T.greenLo, border:`1px solid ${T.green}44`,
            borderRadius:999, fontFamily:'monospace', fontSize:13, fontWeight:800, color:T.green, letterSpacing:'0.1em' }}>{code}</div>
          <button onClick={() => setShowShare(true)} style={{ padding:'6px 14px', borderRadius:999,
            fontSize:12, fontWeight:700, background:T.purpleLo, border:`1px solid ${T.purple}44`, color:T.purple, cursor:'pointer' }}>Share 🔗</button>
        </div>
        <Btn variant="outline" onClick={handleLeave} sm>← Leave</Btn>
      </nav>

      <div style={{ maxWidth:680, margin:'0 auto', padding:'20px 16px 80px', position:'relative', zIndex:1 }}>

        <div style={{ display:'flex', gap:8, marginBottom:14, flexWrap:'wrap' }}>
          {[
            { icon:'🎵', label:'Song',    val: playOrder },
            { icon:'📦', label:'Session', val: `#${sessionNum}` },
            { icon:'🔢', label:'Played',  val: `${songsPlayed % SONGS_PER_SESSION}/${SONGS_PER_SESSION}` },
            { icon:'⏱',  label:'Next ML', val: `${SONGS_PER_SESSION - (songsPlayed % SONGS_PER_SESSION)} songs`, col: T.purple },
          ].map(item => (
            <div key={item.label} style={{ padding:'5px 12px', borderRadius:999,
              background:T.card, border:`1px solid ${T.border}`, fontSize:11, fontWeight:600, color:T.muted }}>
              {item.icon} {item.label} <b style={{ color: item.col || T.text }}>{item.val}</b>
            </div>
          ))}
        </div>

        {isHost && (
          <div className="fade-up" style={{ background:T.purpleLo, borderRadius:14,
            border:`1px solid ${T.purple}33`, padding:'14px 18px', marginBottom:14 }}>
            <div style={{ fontSize:11, fontWeight:700, color:T.purple, letterSpacing:'.08em', marginBottom:10 }}>👑 HOST CONTROLS</div>
            <div style={{ display:'flex', gap:8, flexWrap:'wrap' }}>
              {!isLive && !sessionStarted && (
                <button onClick={handleStartSession}
                  style={{ padding:'7px 14px', borderRadius:8, fontSize:12, fontWeight:600,
                    background:T.greenLo, border:`1px solid ${T.green}44`, color:T.green, cursor:'pointer' }}>
                  🚀 Start Session
                </button>
              )}
              {isLive && (
                <button onClick={() => { handleSongEnd(); toast.success('Skipped!'); }}
                  style={{ padding:'7px 14px', borderRadius:8, fontSize:12, fontWeight:600,
                    background:T.purpleLo, border:`1px solid ${T.purple}44`, color:T.purple, cursor:'pointer' }}>⏭ Skip track</button>
              )}
              <button onClick={() => setShowShare(true)}
                style={{ padding:'7px 14px', borderRadius:8, fontSize:12, fontWeight:600,
                  background:T.greenLo, border:`1px solid ${T.green}44`, color:T.green, cursor:'pointer' }}>🔗 Invite guests</button>
              <button onClick={handleEndRoom}
                style={{ padding:'7px 14px', borderRadius:8, fontSize:12, fontWeight:600,
                  background:'rgba(239,68,68,0.1)', border:'1px solid rgba(239,68,68,0.2)', color:'#EF4444', cursor:'pointer' }}>🚪 End room</button>
            </div>
            {!isLive && sessionStarted && (
              <div style={{ marginTop:10, fontSize:12, color:T.green }}>
                ✅ Pipeline started! Fetching all playlists… wait 2-5 mins
              </div>
            )}
            {!isLive && !sessionStarted && (
              <div style={{ marginTop:10, fontSize:12, color:T.muted }}>
                👆 Wait for guests to join, then click Start Session
              </div>
            )}
          </div>
        )}

        {isLive && now && (
          <div className="fade-up" style={{ background:`linear-gradient(135deg,${T.card},rgba(29,185,84,0.07))`,
            borderRadius:20, border:`1px solid ${T.green}33`, padding:'20px', marginBottom:14 }}>
            <div style={{ fontSize:11, fontWeight:700, color:T.green, textTransform:'uppercase', letterSpacing:'.1em', marginBottom:12 }}>Now playing</div>
            <div style={{ display:'flex', alignItems:'center', gap:14 }}>
              <ArtistImage name={now.artist} image={now.image} size={56} radius={14}
                style={{ border:`2px solid ${T.green}44`, flexShrink:0 }} />
              <div style={{ flex:1, minWidth:0 }}>
                <div style={{ fontWeight:800, fontSize:16, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', marginBottom:3 }}>{now.title}</div>
                <div style={{ color:T.muted, fontSize:13, marginBottom:8, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{now.artist}</div>
                <Waveform playing />
              </div>
              <div style={{ display:'flex', flexDirection:'column', alignItems:'flex-end', gap:6 }}>
                <span style={{ padding:'3px 10px', borderRadius:999, background:T.greenLo,
                  border:`1px solid ${T.green}44`, fontSize:11, fontWeight:700, color:T.green }}>{now.genre}</span>
                <span style={{ fontSize:11, color:T.muted }}>
                  score: <b style={{ color:(now.score||0)>=0?T.green:'#EF4444' }}>{(now.score||0)>0?'+':''}{parseFloat(now.score||0).toFixed(2)}</b>
                </span>
              </div>
            </div>
          </div>
        )}

        <div style={{ marginBottom:14 }}>
          <div style={{ fontSize:11, fontWeight:700, color:T.muted, textTransform:'uppercase', letterSpacing:'.09em', marginBottom:10 }}>
            In the room · {members.length} active
          </div>
          <div style={{ display:'flex', gap:9, flexWrap:'wrap' }}>
            {members.map((m, i) => (
              <div key={m.id||i} style={{ display:'flex', alignItems:'center', gap:8,
                background:T.card, borderRadius:10, padding:'7px 12px',
                border:`1px solid ${m.isHost ? T.purple+'44' : T.green+'33'}` }}>
                <Avatar name={m.av||'U'} size={26} active />
                <span style={{ fontSize:12, fontWeight:600, color:T.text }}>{m.name}</span>
                {m.isHost
                  ? <span style={{ fontSize:10, fontWeight:700, padding:'1px 6px', borderRadius:999,
                      background:T.purpleLo, border:`1px solid ${T.purple}44`, color:T.purple }}>HOST</span>
                  : <span style={{ width:6, height:6, borderRadius:'50%', background:T.green, flexShrink:0, animation:'pulse 2s infinite' }} />
                }
              </div>
            ))}
          </div>
        </div>

        <div style={{ display:'flex', background:T.surface, borderRadius:12, padding:4,
          border:`1px solid ${T.border}`, marginBottom:12 }}>
          {['queue','analytics'].map(t => (
            <button key={t} onClick={() => setRtab(t)} style={{ flex:1, padding:'8px 0',
              borderRadius:9, fontSize:13, fontWeight:600, border:'none', cursor:'pointer',
              background:rtab===t?T.card:'transparent', color:rtab===t?T.text:T.muted, transition:'all .2s' }}>
              {t==='queue' ? '🎵 Queue' : '📊 Analytics'}
            </button>
          ))}
        </div>

        {rtab==='queue' && (
          <div style={{ display:'flex', flexDirection:'column', gap:8 }} className="fade-up">
            {!isLive ? (
              <div style={{ textAlign:'center', padding:'48px 24px', background:T.card,
                borderRadius:16, border:`1px solid ${T.border}` }}>
                <div style={{ fontSize:48, marginBottom:16 }}>🧠</div>
                <div style={{ fontSize:16, fontWeight:700, color:T.text, marginBottom:8 }}>
                  {sessionStarted ? 'Fetching your playlists…' : 'Waiting for host to start session…'}
                </div>
                <div style={{ fontSize:13, color:T.muted, marginBottom:20, lineHeight:1.6 }}>
                  {sessionStarted
                    ? 'ML is analysing everyone\'s taste and building your queue. This takes 2–5 minutes.'
                    : 'The host will start the session once everyone has joined the room.'
                  }
                </div>
                <div style={{ display:'flex', justifyContent:'center', gap:6 }}>
                  {[0,1,2].map(i => (
                    <div key={i} style={{ width:8, height:8, borderRadius:'50%', background:T.green,
                      opacity:0.7, animation:`pulse 1.2s ease-in-out ${i*0.3}s infinite` }} />
                  ))}
                </div>
              </div>
            ) : (
              queue.map((t) => {
                const uid     = user?.uid || 'guest';
                const voteKey = `vote_${sessionId}_${uid}_${t.id}`;
                const hasVoted = localStorage.getItem(voteKey);
                return (
                  <div key={t.id} className="card-hover" style={{
                    background: t.playing ? `linear-gradient(135deg,${T.card},rgba(29,185,84,0.07))` : T.card,
                    borderRadius:14, border:`1px solid ${t.playing ? T.green+'44' : T.border}`,
                    padding:'12px 14px', display:'flex', alignItems:'center', gap:10, transition:'all .2s',
                  }}>
                    <ArtistImage name={t.artist} image={t.image} size={40} radius={10}
                      style={{ border:`1.5px solid ${t.playing ? T.green+'66' : T.border}`, flexShrink:0 }} />
                    <div style={{ flex:1, minWidth:0 }}>
                      <div style={{ fontWeight:700, fontSize:14, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{t.title}</div>
                      <div style={{ color:T.muted, fontSize:12, marginTop:1, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{t.artist}</div>
                    </div>
                    <span style={{ fontSize:11, fontWeight:700, color:(t.score||0)>=0?T.green:'#EF4444', flexShrink:0, marginRight:2 }}>
                      {(t.score||0)>0?'+':''}{(t.score||0).toFixed ? t.score.toFixed(2) : t.score}
                    </span>
                    <div style={{ display:'flex', gap:5, flexShrink:0 }}>
                      <button onClick={() => vote(t.id,'likes')} disabled={!!hasVoted}
                        style={{ display:'flex', alignItems:'center', gap:3, padding:'5px 9px', borderRadius:8,
                          background: hasVoted === 'like' ? T.green+'33' : T.greenLo,
                          border:`1px solid ${T.green}33`, color:T.green, fontSize:12, fontWeight:700,
                          cursor: hasVoted ? 'not-allowed' : 'pointer',
                          opacity: hasVoted && hasVoted !== 'like' ? 0.4 : 1 }}>
                        👍 {t.likes}
                      </button>
                      <button onClick={() => vote(t.id,'dislikes')} disabled={!!hasVoted}
                        style={{ display:'flex', alignItems:'center', gap:3, padding:'5px 9px', borderRadius:8,
                          background: hasVoted === 'dislike' ? 'rgba(239,68,68,0.2)' : 'rgba(239,68,68,0.1)',
                          border:'1px solid rgba(239,68,68,0.2)', color:'#EF4444', fontSize:12, fontWeight:700,
                          cursor: hasVoted ? 'not-allowed' : 'pointer',
                          opacity: hasVoted && hasVoted !== 'dislike' ? 0.4 : 1 }}>
                        👎 {t.dislikes}
                      </button>
                    </div>
                  </div>
                );
              })
            )}
          </div>
        )}

        {rtab==='analytics' && (
          <div className="fade-up" style={{ display:'flex', flexDirection:'column', gap:12 }}>
            <div style={{ background:T.card, borderRadius:16, border:`1px solid ${T.border}`, padding:'18px 20px' }}>
              <div style={{ fontSize:12, fontWeight:700, color:T.muted, textTransform:'uppercase', letterSpacing:'.09em', marginBottom:14 }}>Genre distribution — live</div>
              {genreDist.map(g => <GBar key={g.g} label={g.g} pct={g.p} color={g.c} />)}
            </div>
            <div style={{ background:T.card, borderRadius:16, border:`1px solid ${T.border}`, padding:'18px 20px' }}>
              <div style={{ fontSize:12, fontWeight:700, color:T.muted, textTransform:'uppercase', letterSpacing:'.09em', marginBottom:14 }}>Feedback scores — live</div>
              {scores.map((s, i) => (
                <div key={s.video_id||i} style={{ display:'flex', alignItems:'center', justifyContent:'space-between', padding:'7px 0', borderBottom:`1px solid ${T.border}` }}>
                  <span style={{ fontSize:13, color:T.muted }}>{s.song_name}</span>
                  <span style={{ fontSize:13, fontWeight:700, color:(s.score||0)>=0?T.green:'#EF4444' }}>{(s.score||0)>0?'+':''}{s.score||0}</span>
                </div>
              ))}
            </div>
            <div style={{ background:T.greenLo, borderRadius:14, border:`1px solid ${T.green}33`, padding:'14px 18px' }}>
              <div style={{ fontSize:11, fontWeight:700, color:T.green, letterSpacing:'.08em', marginBottom:4 }}>ML STATUS</div>
              <div style={{ fontSize:13, color:T.muted }}>
                Next recommendations in <b style={{ color:T.green }}>{SONGS_PER_SESSION - (songsPlayed % SONGS_PER_SESSION)} songs</b> · Session <b style={{ color:T.text }}>#{sessionNum}</b>
              </div>
            </div>
            <div style={{ background:T.purpleLo, borderRadius:14, border:`1px solid ${T.purple}33`, padding:'14px 18px' }}>
              <div style={{ fontSize:11, fontWeight:700, color:T.purple, letterSpacing:'.08em', marginBottom:4 }}>PIPELINE STATUS</div>
              <div style={{ fontSize:13, color:T.muted }}>
                Firestore: <b style={{ color:isLive?T.green:T.muted }}>{isLive ? 'Live ✅' : 'Waiting…'}</b>
                {' '}· Events: <b style={{ color:T.text }}>{liveEvents.toLocaleString()}</b>
                {' '}· Songs played: <b style={{ color:T.green }}>{songsPlayed}</b>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
