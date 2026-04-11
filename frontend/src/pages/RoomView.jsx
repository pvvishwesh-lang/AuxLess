import React, { useState, useEffect, useRef } from 'react';
import toast from 'react-hot-toast';
import { T } from '../styles/tokens';
import { MOCK_QUEUE, MOCK_MEMBERS, GENRES_DIST, MOCK_FEEDBACK_SCORES } from '../data/mockData';
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
  collection, doc,
  onSnapshot,
  updateDoc, increment, addDoc,
  serverTimestamp, setDoc, getDoc,
} from 'firebase/firestore';

const WEIGHTS          = { like: 2, dislike: -2, skip: -0.5, replay: 1.5, play: 0, complete: 0.5 };
const SONGS_PER_SESSION = 10;
const SONG_DURATION_MS  = 30000;

// Convert AUX-3CBBE8A2 → 3cbbe8a2 (what ML uses as session_id in Firestore)
const toSessionId = (id) => (id || '').replace(/^AUX-/i, '').toLowerCase();

// ── YouTube Player ────────────────────────────────────────────
function YouTubePlayer({ videoId, onEnd }) {
  if (!videoId) return null;
  return (
    <div style={{ position:'fixed', bottom:80, right:16, zIndex:100,
      borderRadius:12, overflow:'hidden', boxShadow:'0 4px 24px rgba(0,0,0,0.5)',
      border:`1px solid ${T.green}44`, width:240, height:135 }}>
      <iframe
        key={videoId}
        width="240" height="135"
        src={`https://www.youtube.com/embed/${videoId}?autoplay=1&controls=1&rel=0`}
        allow="autoplay; encrypted-media"
        allowFullScreen
        style={{ border:'none', display:'block' }}
        title="Now Playing"
      />
    </div>
  );
}

// ── Share Modal ───────────────────────────────────────────────
function ShareModal({ roomId, onClose }) {
  const link = `https://auxless.app/join/${roomId}`;
  const copy = (text) => {
    navigator.clipboard?.writeText(text).catch(() => {});
    toast.success('Copied!');
  };
  return (
    <div
      style={{ position:'fixed', inset:0, zIndex:200, background:'rgba(0,0,0,0.75)',
        backdropFilter:'blur(8px)', display:'flex', alignItems:'center',
        justifyContent:'center', padding:16 }}
      onClick={onClose}
    >
      <div
        className="fade-up"
        onClick={e => e.stopPropagation()}
        style={{ background:T.card, borderRadius:22, border:`1px solid ${T.border}`,
          padding:'28px 24px', width:'100%', maxWidth:400 }}
      >
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
          {[
            { icon:'💬', label:'WhatsApp', color:'#25D366' },
            { icon:'✈️', label:'Telegram', color:'#2AABEE' },
            { icon:'📱', label:'Messages', color:'#34C759' },
          ].map(p => (
            <button
              key={p.label}
              onClick={() => toast.success(`Opening ${p.label}…`)}
              style={{ flex:1, padding:'10px 4px', borderRadius:10, background:`${p.color}18`,
                border:`1px solid ${p.color}33`, color:p.color, fontSize:12, fontWeight:600,
                cursor:'pointer', display:'flex', flexDirection:'column', alignItems:'center', gap:4 }}
            >
              <span style={{ fontSize:18 }}>{p.icon}</span>{p.label}
            </button>
          ))}
        </div>
        <Btn variant="outline" onClick={onClose} full>Done</Btn>
      </div>
    </div>
  );
}

// ── Main RoomView ─────────────────────────────────────────────
export default function RoomView({ roomId, user, onLeave }) {
  const code      = roomId || 'AUX-7749';
  const sessionId = toSessionId(code); // e.g. "3cbbe8a2" — used for mlDb paths

  const [queue,              setQueue]              = useState(MOCK_QUEUE);
  const [members,            setMembers]            = useState(MOCK_MEMBERS);
  const [genreDist]                                 = useState(GENRES_DIST);
  const [scores,             setScores]             = useState(MOCK_FEEDBACK_SCORES);
  const [liveEvents,         setLiveEvents]         = useState(1284);
  const [progress,           setProgress]           = useState(0);
  const [rtab,               setRtab]               = useState('queue');
  const [showShare,          setShowShare]          = useState(false);
  const [isLive,             setIsLive]             = useState(false);
  const [hostUid,            setHostUid]            = useState(null);
  const [songsPlayed,        setSongsPlayed]        = useState(0);
  const [sessionNum,         setSessionNum]         = useState(1);
  const [playOrder,          setPlayOrder]          = useState(1);
  const [currentSongEventId, setCurrentSongEventId] = useState(null);

  const playOrderRef   = useRef(1);
  const queueRef       = useRef(queue);
  const sessionNumRef  = useRef(1);
  const songsPlayedRef = useRef(0);
  const songEventIdRef = useRef(null);

  useEffect(() => { playOrderRef.current   = playOrder;          }, [playOrder]);
  useEffect(() => { queueRef.current       = queue;              }, [queue]);
  useEffect(() => { sessionNumRef.current  = sessionNum;         }, [sessionNum]);
  useEffect(() => { songsPlayedRef.current = songsPlayed;        }, [songsPlayed]);
  useEffect(() => { songEventIdRef.current = currentSongEventId; }, [currentSongEventId]);

  const isHost = hostUid ? hostUid === user?.uid : false;

  // ── Save room to sessionStorage so refresh stays in room ──
  useEffect(() => {
    if (roomId) sessionStorage.setItem('auxless_room', roomId);
  }, [roomId]);

  // ── Progress bar — 30 sec song timer ─────────────────────
  useEffect(() => {
    const id = setInterval(() => {
      setProgress(p => p >= 100 ? 0 : p + (100 / (SONG_DURATION_MS / 500)));
    }, 500);
    return () => clearInterval(id);
  }, []);

  // ── Song ended when progress resets to 0 ─────────────────
  const prevProgress = useRef(100);
  useEffect(() => {
    if (prevProgress.current > 5 && progress < 2) handleSongEnd();
    prevProgress.current = progress;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [progress]);

  // ── Called when a new song starts ────────────────────────
  const handleSongStart = async (song, order) => {
    if (!roomId || !song) return;
    try {
      // Write song_event to Nikhil's Firestore (uses sessionId inside)
      const eventId = await writeSongEvent({
        roomId,
        song,
        playOrder:  order,
        sessionNum: sessionNumRef.current,
      });
      setCurrentSongEventId(eventId);

      // Publish play event to feedback_events (uses sessionId inside)
      await publishMusicEvent({
        roomId,
        userId:    user?.uid || 'guest',
        songId:    song?.id  || song?.video_id || '',
        eventType: 'play',
        songName:  song?.title  || '',
        artist:    song?.artist || '',
      });

      // Increment songs_played_count in metadata/state (uses sessionId inside)
      await incrementSongsPlayed({ roomId, playOrder: order });

      const newCount = songsPlayedRef.current + 1;
      setSongsPlayed(newCount);

      // Every 10 songs → end session → ML regenerates recommendations
      if (newCount > 0 && newCount % SONGS_PER_SESSION === 0) {
        const newNum = await endSessionFirestore({ roomId, sessionNum: sessionNumRef.current });
        setSessionNum(newNum);
        sessionNumRef.current = newNum;
        toast.success('🧠 ML generating next recommendations…', { duration: 3000 });
      }
    } catch (e) {
      console.warn('handleSongStart failed:', e);
    }
  };

  // ── Called when song finishes (progress hits 100) ─────────
  const handleSongEnd = () => {
    const current = queueRef.current;
    if (!current || current.length === 0) return;

    // Publish complete event
    publishMusicEvent({
      roomId,
      userId:    user?.uid || 'guest',
      songId:    current[0]?.id || '',
      eventType: 'complete',
      songName:  current[0]?.title  || '',
      artist:    current[0]?.artist || '',
    });

    const nextOrder = playOrderRef.current + 1;
    setPlayOrder(nextOrder);
    playOrderRef.current = nextOrder;

    const rotated  = [...current.slice(1), current[0]];
    setQueue(rotated);
    handleSongStart(rotated[0], nextOrder);
    toast(`🎵 Now playing: ${rotated[0]?.title}`, { duration: 2500 });
  };

  // ── Listen to ML recommendations — Nikhil's Firestore ────
  // Uses sessionId (e.g. "3cbbe8a2") NOT code (e.g. "AUX-3CBBE8A2")
  useEffect(() => {
    if (!roomId) return;
    const unsub = onSnapshot(
      collection(mlDb, 'sessions', sessionId, 'recommendations'),
      (snap) => {
        if (snap.empty) return;
        const recs = snap.docs.map((d, i) => {
          // preserve existing votes from current queue
          const existing = queueRef.current.find(q => q.id === d.id);
          return {
            id:       d.id,
            title:    d.data().track_title || d.data().song_name || d.data().title || 'Unknown',
            artist:   d.data().artist_name || d.data().artist   || '',
            genre:    d.data().genre       || '',
            dur:      d.data().dur         || '3:00',
            likes:    existing ? existing.likes    : (d.data().like_count    || 0),
            dislikes: existing ? existing.dislikes : (d.data().dislike_count || 0),
            score:    existing ? existing.score    : (d.data().final_score   || d.data().score || 0),
            playing:  existing ? existing.playing  : i === 0,
            video_id: d.data().video_id    || d.id,
            image:    d.data().artwork_url || d.data().image_url || null,
          };
        });
        setQueue(recs);
        setIsLive(true);
        toast.success('🧠 ML updated your queue!', { duration: 2000 });
      },
      () => {}
    );
    return () => unsub();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [roomId]);

  // ── Listen to track scores — Nikhil's Firestore ──────────
  useEffect(() => {
    if (!roomId) return;
    const unsub = onSnapshot(
      collection(mlDb, 'sessions', sessionId, 'tracks'),
      (snap) => {
        if (snap.empty) return;
        setScores(snap.docs.map(d => ({
          video_id:  d.id,
          song_name: d.data().song_name || d.id,
          score:     d.data().score     || 0,
        })));
        setLiveEvents(prev => prev + snap.docChanges().length);
        setIsLive(true);
      },
      () => {}
    );
    return () => unsub();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [roomId]);

  // ── Start first song on mount ─────────────────────────────
  const startedRef = useRef(false);
  useEffect(() => {
    if (startedRef.current || queue.length === 0) return;
    startedRef.current = true;
    handleSongStart(queue[0], 1);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queue]);

  // ── Room members + host — YOUR Firestore (db) ────────────
  useEffect(() => {
    if (!roomId) return;
    const unsub = onSnapshot(
      doc(db, 'rooms', roomId),
      (snap) => {
        if (!snap.exists()) return;
        const data = snap.data();
        setHostUid(data.host_uid || null);
        const users = data.users || [];
        if (users.length === 0) return;
        setMembers(
          users
            .filter(u => u.isactive !== false)
            .map((u, i) => ({
              id:    i,
              uid:   u.uid,
              name:  u.name   || `User ${i + 1}`,
              av:    u.av     || (u.name || 'U').slice(0, 2).toUpperCase(),
              active: u.isactive !== false,
              isHost: u.isHost || false,
            }))
        );
      },
      () => {}
    );
    return () => unsub();
  }, [roomId]);

  // ── Vote ──────────────────────────────────────────────────
  const vote = async (id, type) => {
    const action = type === 'likes' ? 'like' : 'dislike';
    const delta  = WEIGHTS[action];
    const track  = queue.find(t => t.id === id);

    // Optimistic UI
    setQueue(q => q.map(t =>
      t.id === id
        ? { ...t, [type]: t[type] + 1, score: +(t.score + delta).toFixed(1) }
        : t
    ));
    toast.success(type === 'likes' ? '👍 Voted up!' : '👎 Voted down!');

    // Publish feedback event to Nikhil's Firestore
    await publishMusicEvent({
      roomId,
      userId:    user?.uid || 'guest',
      songId:    id,
      eventType: action,
      songName:  track?.title  || '',
      artist:    track?.artist || '',
    });

    if (roomId) {
      try {
        // Queue score → YOUR Firestore (rooms)
        const qRef = doc(db, 'rooms', roomId, 'queue', id);
        await updateDoc(qRef, {
          score:               increment(delta),
          [`${action}_count`]: increment(1),
          last_updated:        serverTimestamp(),
          song_name:           track?.title  || '',
          artist_name:         track?.artist || '',
        }).catch(async () => {
          await setDoc(qRef, {
            video_id:            id,
            song_name:           track?.title  || '',
            artist_name:         track?.artist || '',
            score:               delta,
            [`${action}_count`]: 1,
            last_updated:        serverTimestamp(),
          });
        });

        // liked_flag → Nikhil's Firestore (song_events)
        if (action === 'like' && songEventIdRef.current) {
          await updateLikedFlag({ roomId, songEventId: songEventIdRef.current });
        }

        // user_feedback → Nikhil's Firestore
        const uid   = user?.uid || 'guest';
        const fbRef = doc(mlDb, 'sessions', sessionId, 'user_feedback', `${uid}_${id}`);
        await updateDoc(fbRef, {
          action,
          score_delta:  delta,
          last_updated: serverTimestamp(),
          user_id:      uid,
          video_id:     id,
        }).catch(async () => {
          await addDoc(collection(mlDb, 'sessions', sessionId, 'user_feedback'), {
            user_id:      uid,
            video_id:     id,
            action,
            score_delta:  delta,
            last_updated: serverTimestamp(),
          });
        });
      } catch (e) {
        console.warn('Vote failed:', e);
      }
    }
  };

  // ── Leave room ────────────────────────────────────────────
  const handleLeave = async () => {
    if (roomId && user?.uid) {
      try {
        const snap = await getDoc(doc(db, 'rooms', roomId));
        if (snap.exists()) {
          const updated = (snap.data().users || []).map(u =>
            u.uid === user.uid ? { ...u, isactive: false } : u
          );
          await updateDoc(doc(db, 'rooms', roomId), { users: updated });
        }
      } catch (e) {}
    }
    sessionStorage.removeItem('auxless_room');
    onLeave();
  };

  const now  = queue.find(t => t.playing) || queue[0];
  const secs = Math.round(progress * (SONG_DURATION_MS / 1000) / 100);

  return (
    <div style={{ minHeight:'100vh', background:T.bg, color:T.text, fontFamily:'system-ui,sans-serif' }}>
      <Blobs />
      {showShare && <ShareModal roomId={code} onClose={() => setShowShare(false)} />}
      <YouTubePlayer videoId={now?.video_id} onEnd={handleSongEnd} />

      {/* ── Nav ── */}
      <nav style={{ position:'sticky', top:0, zIndex:50, display:'flex', alignItems:'center',
        justifyContent:'space-between', padding:'0 20px', height:58,
        borderBottom:`1px solid ${T.border}`, backdropFilter:'blur(18px)',
        background:'rgba(8,8,15,.88)' }}>
        <div style={{ display:'flex', alignItems:'center', gap:9 }}>
          <div style={{ width:28, height:28, borderRadius:8,
            background:`linear-gradient(135deg,${T.green},${T.purple})`,
            display:'flex', alignItems:'center', justifyContent:'center', fontSize:14 }}>♫</div>
          <span style={{ fontWeight:900, fontSize:15, letterSpacing:'-0.4px' }}>AuxLess</span>
          {isHost && (
            <span style={{ padding:'2px 8px', borderRadius:999, background:T.purpleLo,
              border:`1px solid ${T.purple}44`, fontSize:10, fontWeight:700, color:T.purple }}>
              👑 HOST
            </span>
          )}
        </div>
        <div style={{ display:'flex', alignItems:'center', gap:8 }}>
          {isLive && (
            <div style={{ display:'flex', alignItems:'center', gap:5, padding:'3px 10px',
              borderRadius:999, background:T.greenLo, border:`1px solid ${T.green}44`,
              fontSize:10, fontWeight:700, color:T.green }}>
              <span style={{ width:5, height:5, borderRadius:'50%', background:T.green,
                animation:'pulse 2s infinite' }} />LIVE
            </div>
          )}
          <div style={{ padding:'5px 14px', background:T.greenLo, border:`1px solid ${T.green}44`,
            borderRadius:999, fontFamily:'monospace', fontSize:13, fontWeight:800,
            color:T.green, letterSpacing:'0.1em' }}>{code}</div>
          <button onClick={() => setShowShare(true)} style={{ padding:'6px 14px', borderRadius:999,
            fontSize:12, fontWeight:700, background:T.purpleLo, border:`1px solid ${T.purple}44`,
            color:T.purple, cursor:'pointer' }}>Share 🔗</button>
        </div>
        <Btn variant="outline" onClick={handleLeave} sm>← Leave</Btn>
      </nav>

      <div style={{ maxWidth:680, margin:'0 auto', padding:'20px 16px 80px', position:'relative', zIndex:1 }}>

        {/* ── Session info bar ── */}
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

        {/* ── Host controls ── */}
        {isHost && (
          <div className="fade-up" style={{ background:T.purpleLo, borderRadius:14,
            border:`1px solid ${T.purple}33`, padding:'14px 18px', marginBottom:14 }}>
            <div style={{ fontSize:11, fontWeight:700, color:T.purple,
              letterSpacing:'.08em', marginBottom:10 }}>👑 HOST CONTROLS</div>
            <div style={{ display:'flex', gap:8, flexWrap:'wrap' }}>
              <button onClick={() => { handleSongEnd(); toast.success('Skipped!'); }}
                style={{ padding:'7px 14px', borderRadius:8, fontSize:12, fontWeight:600,
                  background:T.purpleLo, border:`1px solid ${T.purple}44`, color:T.purple, cursor:'pointer' }}>
                ⏭ Skip track
              </button>
              <button onClick={() => setShowShare(true)}
                style={{ padding:'7px 14px', borderRadius:8, fontSize:12, fontWeight:600,
                  background:T.greenLo, border:`1px solid ${T.green}44`, color:T.green, cursor:'pointer' }}>
                🔗 Invite guests
              </button>
              <button onClick={() => { toast.success('Room ended!'); handleLeave(); }}
                style={{ padding:'7px 14px', borderRadius:8, fontSize:12, fontWeight:600,
                  background:'rgba(239,68,68,0.1)', border:'1px solid rgba(239,68,68,0.2)',
                  color:'#EF4444', cursor:'pointer' }}>
                🚪 End room
              </button>
            </div>
          </div>
        )}

        {/* ── Now playing ── */}
        <div className="fade-up" style={{ background:`linear-gradient(135deg,${T.card},rgba(29,185,84,0.07))`,
          borderRadius:20, border:`1px solid ${T.green}33`, padding:'20px 20px', marginBottom:14 }}>
          <div style={{ fontSize:11, fontWeight:700, color:T.green,
            textTransform:'uppercase', letterSpacing:'.1em', marginBottom:12 }}>Now playing</div>
          <div style={{ display:'flex', alignItems:'center', gap:14, marginBottom:16 }}>
            <ArtistImage name={now?.artist} image={now?.image} size={56} radius={14}
              style={{ border:`2px solid ${T.green}44`, flexShrink:0 }} />
            <div style={{ flex:1, minWidth:0 }}>
              <div style={{ fontWeight:800, fontSize:16, letterSpacing:'-0.3px',
                overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', marginBottom:3 }}>
                {now?.title}
              </div>
              <div style={{ color:T.muted, fontSize:13, marginBottom:8,
                overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>
                {now?.artist}
              </div>
              <Waveform playing />
            </div>
            <div style={{ display:'flex', flexDirection:'column', alignItems:'flex-end', gap:6 }}>
              <span style={{ padding:'3px 10px', borderRadius:999, background:T.greenLo,
                border:`1px solid ${T.green}44`, fontSize:11, fontWeight:700, color:T.green }}>
                {now?.genre}
              </span>
              <span style={{ fontSize:11, color:T.muted }}>
                score: <b style={{ color:(now?.score||0)>=0?T.green:'#EF4444' }}>
                  {(now?.score||0)>0?'+':''}{now?.score||0}
                </b>
              </span>
            </div>
          </div>
          <div style={{ background:'rgba(255,255,255,0.06)', borderRadius:6, height:5, overflow:'hidden', marginBottom:5 }}>
            <div style={{ width:`${progress}%`, height:5, borderRadius:6,
              background:`linear-gradient(90deg,${T.green},${T.purple})`, transition:'width .5s linear' }} />
          </div>
          <div style={{ display:'flex', justifyContent:'space-between' }}>
            <span style={{ fontSize:11, color:T.dim }}>{Math.floor(secs/60)}:{String(secs%60).padStart(2,'0')}</span>
            <span style={{ fontSize:11, color:T.dim }}>0:30</span>
          </div>
        </div>

        {/* ── Members ── */}
        <div style={{ marginBottom:14 }}>
          <div style={{ fontSize:11, fontWeight:700, color:T.muted,
            textTransform:'uppercase', letterSpacing:'.09em', marginBottom:10 }}>
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
                  : <span style={{ width:6, height:6, borderRadius:'50%', background:T.green,
                      flexShrink:0, animation:'pulse 2s infinite' }} />
                }
              </div>
            ))}
          </div>
        </div>

        {/* ── Tabs ── */}
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

        {/* ── Queue ── */}
        {rtab==='queue' && (
          <div style={{ display:'flex', flexDirection:'column', gap:8 }} className="fade-up">
            {queue.map((t) => (
              <div key={t.id} className="card-hover" style={{
                background: t.playing
                  ? `linear-gradient(135deg,${T.card},rgba(29,185,84,0.07))`
                  : T.card,
                borderRadius:14,
                border:`1px solid ${t.playing ? T.green+'44' : T.border}`,
                padding:'12px 14px', display:'flex', alignItems:'center', gap:10, transition:'all .2s',
              }}>
                <ArtistImage name={t.artist} image={t.image} size={40} radius={10}
                  style={{ border:`1.5px solid ${t.playing ? T.green+'66' : T.border}`, flexShrink:0 }} />
                <div style={{ flex:1, minWidth:0 }}>
                  <div style={{ fontWeight:700, fontSize:14, overflow:'hidden',
                    textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{t.title}</div>
                  <div style={{ color:T.muted, fontSize:12, marginTop:1,
                    overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{t.artist}</div>
                </div>
                <span style={{ fontSize:11, fontWeight:700,
                  color:(t.score||0)>=0?T.green:'#EF4444', flexShrink:0, marginRight:2 }}>
                  {(t.score||0)>0?'+':''}{t.score||0}
                </span>
                <div style={{ display:'flex', gap:5, flexShrink:0 }}>
                  <button onClick={() => vote(t.id,'likes')} style={{ display:'flex', alignItems:'center',
                    gap:3, padding:'5px 9px', borderRadius:8, background:T.greenLo,
                    border:`1px solid ${T.green}33`, color:T.green, fontSize:12, fontWeight:700, cursor:'pointer' }}>
                    👍 {t.likes}
                  </button>
                  <button onClick={() => vote(t.id,'dislikes')} style={{ display:'flex', alignItems:'center',
                    gap:3, padding:'5px 9px', borderRadius:8, background:'rgba(239,68,68,0.1)',
                    border:'1px solid rgba(239,68,68,0.2)', color:'#EF4444', fontSize:12, fontWeight:700, cursor:'pointer' }}>
                    👎 {t.dislikes}
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}

        {/* ── Analytics ── */}
        {rtab==='analytics' && (
          <div className="fade-up" style={{ display:'flex', flexDirection:'column', gap:12 }}>
            <div style={{ background:T.card, borderRadius:16, border:`1px solid ${T.border}`, padding:'18px 20px' }}>
              <div style={{ fontSize:12, fontWeight:700, color:T.muted,
                textTransform:'uppercase', letterSpacing:'.09em', marginBottom:14 }}>
                Genre distribution — live
              </div>
              {genreDist.map(g => <GBar key={g.g} label={g.g} pct={g.p} color={g.c} />)}
            </div>

            <div style={{ background:T.card, borderRadius:16, border:`1px solid ${T.border}`, padding:'18px 20px' }}>
              <div style={{ fontSize:12, fontWeight:700, color:T.muted,
                textTransform:'uppercase', letterSpacing:'.09em', marginBottom:14 }}>
                Feedback scores — live from pipeline
              </div>
              {scores.map((s, i) => (
                <div key={s.video_id||i} style={{ display:'flex', alignItems:'center',
                  justifyContent:'space-between', padding:'7px 0', borderBottom:`1px solid ${T.border}` }}>
                  <span style={{ fontSize:13, color:T.muted }}>{s.song_name}</span>
                  <span style={{ fontSize:13, fontWeight:700, color:(s.score||0)>=0?T.green:'#EF4444' }}>
                    {(s.score||0)>0?'+':''}{s.score||0}
                  </span>
                </div>
              ))}
            </div>

            <div style={{ background:T.greenLo, borderRadius:14, border:`1px solid ${T.green}33`, padding:'14px 18px' }}>
              <div style={{ fontSize:11, fontWeight:700, color:T.green, letterSpacing:'.08em', marginBottom:4 }}>
                ML STATUS
              </div>
              <div style={{ fontSize:13, color:T.muted }}>
                Next recommendations in <b style={{ color:T.green }}>
                  {SONGS_PER_SESSION - (songsPlayed % SONGS_PER_SESSION)} songs
                </b> · Session <b style={{ color:T.text }}>#{sessionNum}</b>
              </div>
            </div>

            <div style={{ background:T.purpleLo, borderRadius:14, border:`1px solid ${T.purple}33`, padding:'14px 18px' }}>
              <div style={{ fontSize:11, fontWeight:700, color:T.purple, letterSpacing:'.08em', marginBottom:4 }}>
                PIPELINE STATUS
              </div>
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