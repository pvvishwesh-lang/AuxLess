import React, { useState } from 'react';
import { T } from '../styles/tokens';
import Blobs from '../components/Blobs';

export default function SettingsPage({ user, onLogout }) {
  const [notifications, setNotifications] = useState(true);
  const [autoJoin,      setAutoJoin]      = useState(false);
  const [quality,       setQuality]       = useState('high');

  const Toggle = ({ on, onClick }) => (
    <div onClick={onClick} style={{ width: 42, height: 24, borderRadius: 12, background: on ? T.green : 'rgba(255,255,255,0.1)', cursor: 'pointer', position: 'relative', transition: 'background .25s' }}>
      <div style={{ position: 'absolute', top: 3, left: on ? 21 : 3, width: 18, height: 18, borderRadius: '50%', background: '#fff', transition: 'left .25s' }} />
    </div>
  );

  return (
    <div className="fade-up" style={{ maxWidth: 500, margin: '48px auto', padding: '0 24px' }}>
      <Blobs />
      <div style={{ position: 'relative', zIndex: 1 }}>
        <h2 style={{ fontSize: 24, fontWeight: 900, color: T.text, letterSpacing: '-0.5px', marginBottom: 24 }}>Settings</h2>

        {/* preferences */}
        <div style={{ background: T.card, borderRadius: 18, border: `1px solid ${T.border}`, padding: '20px 22px', marginBottom: 14 }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: T.muted, textTransform: 'uppercase', letterSpacing: '.1em', marginBottom: 14 }}>Preferences</div>
          {[
            { label: 'Room notifications', sub: 'Get notified when someone joins', on: notifications, set: setNotifications },
            { label: 'Auto-join last room', sub: 'Rejoin your last room on login',  on: autoJoin,      set: setAutoJoin      },
          ].map(s => (
            <div key={s.label} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '12px 0', borderBottom: `1px solid ${T.border}` }}>
              <div>
                <div style={{ fontSize: 14, fontWeight: 600, color: T.text }}>{s.label}</div>
                <div style={{ fontSize: 12, color: T.muted, marginTop: 2 }}>{s.sub}</div>
              </div>
              <Toggle on={s.on} onClick={() => s.set(p => !p)} />
            </div>
          ))}
          {/* quality selector */}
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '12px 0' }}>
            <div>
              <div style={{ fontSize: 14, fontWeight: 600, color: T.text }}>Stream quality</div>
              <div style={{ fontSize: 12, color: T.muted, marginTop: 2 }}>Audio quality for room playback</div>
            </div>
            <select value={quality} onChange={e => setQuality(e.target.value)} style={{ background: T.surface, border: `1px solid ${T.border}`, borderRadius: 8, color: T.text, fontSize: 13, padding: '6px 10px', cursor: 'pointer' }}>
              <option value="low">Low</option>
              <option value="medium">Medium</option>
              <option value="high">High</option>
            </select>
          </div>
        </div>

        {/* account */}
        <div style={{ background: T.card, borderRadius: 18, border: `1px solid ${T.border}`, padding: '20px 22px', marginBottom: 14 }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: T.muted, textTransform: 'uppercase', letterSpacing: '.1em', marginBottom: 14 }}>Account</div>
          <div style={{ display: 'flex', justifyContent: 'space-between', padding: '10px 0', borderBottom: `1px solid ${T.border}` }}>
            <span style={{ fontSize: 13, color: T.muted }}>Logged in as</span>
            <span style={{ fontSize: 13, fontWeight: 600, color: T.text }}>{user?.email || '—'}</span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', padding: '10px 0' }}>
            <span style={{ fontSize: 13, color: T.muted }}>Auth provider</span>
            <span style={{ fontSize: 13, fontWeight: 600, color: T.text }}>Google</span>
          </div>
        </div>

        {/* sign out */}
        <button onClick={onLogout} style={{ width: '100%', padding: '13px', borderRadius: 12, background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.2)', color: '#EF4444', fontSize: 14, fontWeight: 600, cursor: 'pointer' }}>
          🚪 Sign out
        </button>
      </div>
    </div>
  );
}