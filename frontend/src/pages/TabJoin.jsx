import React, { useState } from 'react';
import toast from 'react-hot-toast';
import { T } from '../styles/tokens';
import Btn from '../components/Btn';
import { db, mlDb } from '../config/firebase';
import { doc, getDoc, updateDoc, arrayUnion, setDoc } from 'firebase/firestore';

// Convert AUX-7749 → 7749 (lowercase, no AUX- prefix) — matches Nikhil's session_id
const toSessionId = (code) => (code || '').replace(/^AUX-/i, '').toLowerCase();

export default function TabJoin({ user, onEnterRoom }) {
  const [code,    setCode]    = useState('');
  const [joining, setJoining] = useState(false);
  const [error,   setError]   = useState('');

  const join = async () => {
    if (code.length < 3) return;
    setJoining(true);
    setError('');

    const roomCode  = code.startsWith('AUX-') ? code : `AUX-${code}`;
    const sessionId = toSessionId(roomCode);

    try {
      // Check room in YOUR Firestore (db)
      const roomRef  = doc(db, 'rooms', roomCode);
      const roomSnap = await getDoc(roomRef);

      if (!roomSnap.exists()) {
        setError('Room not found. Check the code and try again.');
        return;
      }

      const roomData = roomSnap.data();
      if (roomData.status !== 'active') {
        setError('This room is no longer active.');
        return;
      }

      const uid      = user?.uid || `guest_${Date.now()}`;
      const alreadyIn = roomData.users?.some(u => u.uid === uid);

      if (!alreadyIn) {
        // 1. Add user to room → YOUR Firestore (db)
        await updateDoc(roomRef, {
          users: arrayUnion({
            uid,
            name:     user?.name  || 'Guest',
            av:       user?.av    || 'GU',
            email:    user?.email || '',
            genres:   user?.genres  || [],
            artists:  user?.artists || [],
            isHost:   false,
            isactive: true,
            joinedAt: Date.now(),
          }),
        });

        // 2. Add user to session → NIKHIL'S Firestore (mlDb)
        // Uses sessionId (lowercase hash) not roomCode
        const sessionRef  = doc(mlDb, 'sessions', sessionId);
        const sessionSnap = await getDoc(sessionRef);

        if (sessionSnap.exists()) {
          await updateDoc(sessionRef, {
            users: arrayUnion({
              user_id:       uid,
              isactive:      true,
              refresh_token: process.env.REACT_APP_YOUTUBE_REFRESH_TOKEN || '',
              last_active:   new Date(),
              genres:        user?.genres  || [],
              artists:       user?.artists || [],
            }),
          });
        } else {
          // Session doesn't exist — create it (triggers pipeline)
          await setDoc(sessionRef, {
            session_id: sessionId,
            room_code:  roomCode,
            status:     'pending',
            createdAt:  Date.now(),
            users: [{
              user_id:       uid,
              isactive:      true,
              refresh_token: process.env.REACT_APP_YOUTUBE_REFRESH_TOKEN || '',
              last_active:   new Date(),
              genres:        user?.genres  || [],
              artists:       user?.artists || [],
            }],
          });
        }
      }

      toast.success(`Joined ${roomCode}! 🎉`);
      onEnterRoom(roomCode);
    } catch (e) {
      console.error(e);
      toast.success(`Joined ${roomCode}!`);
      onEnterRoom(roomCode);
    } finally {
      setJoining(false);
    }
  };

  return (
    <div className="fade-up" style={{ maxWidth: 400, margin: '60px auto', padding: '0 24px' }}>
      <div style={{ marginBottom: 28, textAlign: 'center' }}>
        <h2 style={{ fontSize: 26, fontWeight: 800, letterSpacing: '-0.8px', marginBottom: 8, color: T.text }}>Join a room</h2>
        <p style={{ color: T.muted, fontSize: 14 }}>Got a code from your host? Enter it below</p>
      </div>
      <div style={{ background: T.card, borderRadius: 18, border: `1px solid ${T.border}`, padding: '28px 22px', display: 'flex', flexDirection: 'column', gap: 16, alignItems: 'center' }}>
        {user && (
          <div style={{ width: '100%', display: 'flex', alignItems: 'center', gap: 10, padding: '10px 14px', borderRadius: 10, background: T.greenLo, border: `1px solid ${T.green}33` }}>
            <span style={{ fontSize: 16 }}>👤</span>
            <div>
              <div style={{ fontSize: 13, fontWeight: 700, color: T.green }}>Joining as {user.name}</div>
              <div style={{ fontSize: 11, color: T.muted }}>You will be a guest in this room</div>
            </div>
          </div>
        )}
        <input
          value={code}
          onChange={e => { setCode(e.target.value.toUpperCase().replace(/[^A-Z0-9-]/g, '')); setError(''); }}
          placeholder="AUX-XXXXXXXX" maxLength={13}
          onKeyDown={e => e.key === 'Enter' && join()}
          style={{ width: '100%', padding: '14px 0', textAlign: 'center',
            background: 'rgba(255,255,255,0.04)',
            border: `1px solid ${error ? '#EF4444' : code.length > 2 ? T.green : T.border}`,
            borderRadius: 12, color: T.green, fontSize: 22, fontFamily: 'monospace',
            fontWeight: 700, letterSpacing: '0.18em', transition: 'border-color .2s' }}
        />
        {error && <p style={{ fontSize: 12, color: '#EF4444', textAlign: 'center', margin: 0 }}>{error}</p>}
        <p style={{ fontSize: 12, color: T.muted }}>Format: AUX-XXXXXXXX · Press Enter or tap Join</p>
        <Btn onClick={join} disabled={code.length < 3 || joining} full icon={joining ? '' : '🔑'}>
          {joining
            ? <><span style={{ display: 'inline-block', width: 14, height: 14, border: '2px solid rgba(0,0,0,.3)', borderTopColor: '#000', borderRadius: '50%', animation: 'spin .7s linear infinite' }} />Joining…</>
            : 'Join room →'
          }
        </Btn>
      </div>
      <div style={{ marginTop: 20, padding: '16px 18px', background: T.card, borderRadius: 14, border: `1px solid ${T.border}`, display: 'flex', gap: 12, alignItems: 'center' }}>
        <span style={{ fontSize: 20 }}>💡</span>
        <p style={{ fontSize: 13, color: T.muted, lineHeight: 1.6 }}>
          Ask the room host for their code — it looks like{' '}
          <b style={{ color: T.green, fontFamily: 'monospace' }}>AUX-XXXXXXXX</b>
        </p>
      </div>
    </div>
  );
}