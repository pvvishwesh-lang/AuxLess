import React from 'react';
import { T } from '../styles/tokens';
import Avatar from '../components/Avatar';
import Blobs from '../components/Blobs';

export default function ProfilePage({ user }) {
  const joinDate = new Date().toLocaleDateString('en-US', { month: 'long', year: 'numeric' });

  return (
    <div className="fade-up" style={{ maxWidth: 500, margin: '48px auto', padding: '0 24px' }}>
      <Blobs />
      <div style={{ position: 'relative', zIndex: 1 }}>

        {/* header */}
        <div style={{ textAlign: 'center', marginBottom: 28 }}>
          <div style={{ margin: '0 auto 16px', width: 80, height: 80 }}>
            <Avatar name={user?.av || 'U'} size={80} active />
          </div>
          <h2 style={{ fontSize: 24, fontWeight: 900, color: T.text, letterSpacing: '-0.5px', marginBottom: 4 }}>
            {user?.name || 'Guest'}
          </h2>
          <p style={{ fontSize: 13, color: T.muted }}>Joined {joinDate}</p>
          <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6, marginTop: 8, padding: '4px 12px', borderRadius: 999, background: T.greenLo, border: `1px solid ${T.green}44` }}>
            <span style={{ width: 6, height: 6, borderRadius: '50%', background: T.green }} />
            <span style={{ fontSize: 12, fontWeight: 600, color: T.green }}>Active</span>
          </div>
        </div>

        {/* info card */}
        <div style={{ background: T.card, borderRadius: 18, border: `1px solid ${T.border}`, padding: '20px 22px', marginBottom: 14 }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: T.muted, textTransform: 'uppercase', letterSpacing: '.1em', marginBottom: 14 }}>Account info</div>
          {[
            { label: 'Name',     value: user?.name  || '—' },
            { label: 'Email',    value: user?.email || '—' },
            { label: 'Auth',     value: 'Google OAuth 2.0' },
            { label: 'Token',    value: 'JWT · stored locally' },
          ].map(r => (
            <div key={r.label} style={{ display: 'flex', justifyContent: 'space-between', padding: '10px 0', borderBottom: `1px solid ${T.border}` }}>
              <span style={{ fontSize: 13, color: T.muted }}>{r.label}</span>
              <span style={{ fontSize: 13, fontWeight: 600, color: T.text }}>{r.value}</span>
            </div>
          ))}
        </div>

        {/* genres + artists */}
        {(user?.genres?.length > 0 || user?.artists?.length > 0) && (
          <div style={{ background: T.card, borderRadius: 18, border: `1px solid ${T.border}`, padding: '20px 22px' }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: T.muted, textTransform: 'uppercase', letterSpacing: '.1em', marginBottom: 14 }}>Your taste</div>
            {user?.genres?.length > 0 && (
              <div style={{ marginBottom: 12 }}>
                <div style={{ fontSize: 12, color: T.muted, marginBottom: 8 }}>Genres</div>
                <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                  {user.genres.map(g => (
                    <span key={g} style={{ padding: '3px 10px', borderRadius: 999, background: T.greenLo, border: `1px solid ${T.green}33`, fontSize: 12, fontWeight: 600, color: T.green }}>{g}</span>
                  ))}
                </div>
              </div>
            )}
            {user?.artists?.length > 0 && (
              <div>
                <div style={{ fontSize: 12, color: T.muted, marginBottom: 8 }}>Artists</div>
                <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                  {user.artists.map(a => (
                    <span key={a} style={{ padding: '3px 10px', borderRadius: 999, background: T.purpleLo, border: `1px solid ${T.purple}33`, fontSize: 12, fontWeight: 600, color: T.purple }}>{a}</span>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}