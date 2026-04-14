import React, { useState } from 'react';
import toast from 'react-hot-toast';
import { T } from '../styles/tokens';
import Btn from '../components/Btn';
import { db, mlDb } from '../config/firebase';
import { doc, setDoc } from 'firebase/firestore';

export default function TabCreate({ user, onEnterRoom }) {
  const [name,     setName]     = useState('');
  const [priv,     setPriv]     = useState(false);
  const [creating, setCreating] = useState(false);
  const [done,     setDone]     = useState(false);
  const [roomCode, setRoomCode] = useState('');

  const sha256Short = async (str) => {
    const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(str));
    const hex = Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, '0')).join('');
    return hex.slice(0, 4).toUpperCase();
  };

  const create = async () => {
    if (!name.trim()) return;
    setCreating(true);
    try {
      const raw     = `${user?.uid || 'guest'}-${Date.now()}`;
      const hash    = await sha256Short(raw);
      const code    = 'AUX-' + hash;
      const session = hash.toLowerCase();

      // 1. YOUR Firestore — await this
      await setDoc(doc(db, 'rooms', code), {
        code,
        name:       name.trim(),
        session_id: code,
        host_uid:   user?.uid  || 'guest',
        host_name:  user?.name || 'Host',
        host_av:    user?.av   || 'HO',
        private:    priv,
        status:     'active',
        createdAt:  Date.now(),
        users: [{
          uid:      user?.uid   || 'guest',
          name:     user?.name  || 'Host',
          av:       user?.av    || 'HO',
          email:    user?.email || '',
          genres:   user?.genres  || [],
          artists:  user?.artists || [],
          isHost:   true,
          isactive: true,
          joinedAt: Date.now(),
        }],
      });

      // 2. ML Firestore — NO await, background
      setDoc(doc(mlDb, 'sessions', session), {
        session_id:    session,
        room_code:     code,
        status:        'pending',
        createdAt:     Date.now(),
        users: [{
          user_id:       user?.uid  || 'guest',
          isactive:      true,
          refresh_token: process.env.REACT_APP_YOUTUBE_REFRESH_TOKEN || '',
          last_active:   new Date(),
          genres:        user?.genres  || [],
          artists:       user?.artists || [],
        }],
      }).catch(e => console.warn('mlDb session failed:', e));

      // 3. ML metadata — NO await, background
      setDoc(doc(mlDb, 'sessions', session, 'metadata', 'state'), {
        songs_played_count: 0,
        session_number:     1,
        current_session_id: session,
        status:             'active',
        createdAt:          Date.now(),
      }).catch(e => console.warn('mlDb metadata failed:', e));

      setRoomCode(code);
      setDone(true);
      toast.success('Room created!');
    } catch (e) {
      console.error(e);
      toast.error('Failed to create room');
    } finally {
      setCreating(false);
    }
  };

  if (done) return (
    <div className="fade-up" style={{ maxWidth: 440, margin: '60px auto', padding: '0 24px' }}>
      <div style={{ background: T.card, borderRadius: 22, border: `1px solid ${T.green}44`, padding: '32px 28px', textAlign: 'center' }}>
        <div style={{ fontSize: 48, marginBottom: 12 }}>🎛️</div>
        <h2 style={{ fontSize: 22, fontWeight: 800, letterSpacing: '-0.5px', marginBottom: 6, color: T.text }}>Room created!</h2>
        <p style={{ color: T.muted, fontSize: 14, marginBottom: 8 }}>
          You are the <span style={{ color: T.purple, fontWeight: 700 }}>HOST</span> — share this code with your guests
        </p>
        <div style={{ background: T.surface, borderRadius: 12, padding: '14px 0', fontFamily: 'monospace', fontSize: 28, fontWeight: 800, letterSpacing: '0.12em', color: T.green, marginBottom: 12, border: `1px solid ${T.green}33` }}>
          {roomCode}
        </div>
        <button
          onClick={() => { navigator.clipboard?.writeText(roomCode); toast.success('Code copied!'); }}
          style={{ padding: '8px 20px', borderRadius: 8, marginBottom: 20, background: T.greenLo, border: `1px solid ${T.green}44`, color: T.green, fontSize: 13, fontWeight: 600, cursor: 'pointer' }}
        >📋 Copy code</button>
        {(user?.genres?.length > 0 || user?.artists?.length > 0) && (
          <div style={{ background: T.purpleLo, borderRadius: 12, padding: '14px 16px', border: `1px solid ${T.purple}33`, marginBottom: 20, textAlign: 'left' }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: T.purple, textTransform: 'uppercase', letterSpacing: '.08em', marginBottom: 10 }}>🧠 Your taste seeds the ML</div>
            {user?.genres?.length > 0 && (
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 8 }}>
                {user.genres.slice(0, 4).map(g => (
                  <span key={g} style={{ padding: '3px 10px', borderRadius: 999, background: T.greenLo, border: `1px solid ${T.green}33`, fontSize: 11, fontWeight: 600, color: T.green }}>{g}</span>
                ))}
              </div>
            )}
            {user?.artists?.length > 0 && (
              <div style={{ fontSize: 12, color: T.muted }}>Artists: <b style={{ color: T.text }}>{user.artists.slice(0, 3).join(', ')}</b></div>
            )}
            <div style={{ fontSize: 11, color: T.muted, marginTop: 8 }}>Pipeline triggered! Fetching playlists + running ML… 🚀</div>
          </div>
        )}
        <Btn onClick={() => onEnterRoom(roomCode)} full icon="🎵">Enter room →</Btn>
      </div>
    </div>
  );

  return (
    <div className="fade-up" style={{ maxWidth: 440, margin: '60px auto', padding: '0 24px' }}>
      <div style={{ marginBottom: 28, textAlign: 'center' }}>
        <h2 style={{ fontSize: 26, fontWeight: 800, letterSpacing: '-0.8px', marginBottom: 8, color: T.text }}>Create a room</h2>
        <p style={{ color: T.muted, fontSize: 14 }}>You'll be the <b style={{ color: T.purple }}>HOST</b> — guests join with your code</p>
      </div>
      <div style={{ background: T.card, borderRadius: 18, border: `1px solid ${T.border}`, padding: '24px 22px', display: 'flex', flexDirection: 'column', gap: 16 }}>
        {user && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '12px 14px', borderRadius: 10, background: T.purpleLo, border: `1px solid ${T.purple}33` }}>
            <span style={{ fontSize: 20 }}>👑</span>
            <div>
              <div style={{ fontSize: 13, fontWeight: 700, color: T.purple }}>You will be the HOST</div>
              <div style={{ fontSize: 11, color: T.muted }}>Logged in as {user.name}</div>
            </div>
          </div>
        )}
        <div style={{ padding: '12px 14px', borderRadius: 10, background: T.greenLo, border: `1px solid ${T.green}33` }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: T.green, textTransform: 'uppercase', letterSpacing: '.08em', marginBottom: 8 }}>🧠 How AuxLess works</div>
          <div style={{ fontSize: 12, color: T.muted, lineHeight: 1.7 }}>
            Every guest's playlist gets fetched → ML clusters <b style={{ color: T.text }}>everyone's taste</b> together → songs from the dominant genre play → live feedback reorders the queue in real-time
          </div>
        </div>
        <div>
          <label style={{ fontSize: 12, fontWeight: 600, color: T.muted, textTransform: 'uppercase', letterSpacing: '.08em', display: 'block', marginBottom: 8 }}>Room name</label>
          <input
            value={name} onChange={e => setName(e.target.value)} maxLength={40}
            placeholder="Friday night party 🎉"
            style={{ width: '100%', padding: '11px 14px', background: 'rgba(255,255,255,0.04)', border: `1px solid ${T.border}`, borderRadius: 10, color: T.text, fontSize: 14, transition: 'border-color .2s' }}
            onFocus={e => e.target.style.borderColor = T.green}
            onBlur={e  => e.target.style.borderColor = T.border}
          />
          <div style={{ fontSize: 11, color: T.dim, marginTop: 4, textAlign: 'right' }}>{name.length}/40</div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '12px 14px', background: 'rgba(255,255,255,0.03)', borderRadius: 10, border: `1px solid ${T.border}` }}>
          <div>
            <div style={{ fontSize: 14, fontWeight: 600, color: T.text }}>Private room</div>
            <div style={{ fontSize: 12, color: T.muted, marginTop: 2 }}>Invite-only with code</div>
          </div>
          <div onClick={() => setPriv(p => !p)} style={{ width: 42, height: 24, borderRadius: 12, background: priv ? T.green : 'rgba(255,255,255,0.1)', cursor: 'pointer', position: 'relative', transition: 'background .25s' }}>
            <div style={{ position: 'absolute', top: 3, left: priv ? 21 : 3, width: 18, height: 18, borderRadius: '50%', background: '#fff', transition: 'left .25s' }} />
          </div>
        </div>
        <Btn onClick={create} disabled={!name.trim() || creating} full icon={creating ? '' : '🎛️'}>
          {creating
            ? <><span style={{ display: 'inline-block', width: 14, height: 14, border: '2px solid rgba(0,0,0,.3)', borderTopColor: '#000', borderRadius: '50%', animation: 'spin .7s linear infinite' }} />Creating…</>
            : 'Create room'
          }
        </Btn>
      </div>
    </div>
  );
}