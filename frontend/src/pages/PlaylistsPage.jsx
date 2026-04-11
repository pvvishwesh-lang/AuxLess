import React from 'react';
import { T } from '../styles/tokens';
import Blobs from '../components/Blobs';

const MOCK_PLAYLISTS = [
  { id:1, name:'Liked Videos',    tracks:240, genre:'Mixed'    },
  { id:2, name:'Hip-Hop Hits',    tracks:58,  genre:'Hip-Hop'  },
  { id:3, name:'Late Night Vibes',tracks:34,  genre:'R&B'      },
  { id:4, name:'Party Bangers',   tracks:92,  genre:'Pop'      },
];

export default function PlaylistsPage({ user }) {
  return (
    <div className="fade-up" style={{ maxWidth: 540, margin: '48px auto', padding: '0 24px' }}>
      <Blobs />
      <div style={{ position: 'relative', zIndex: 1 }}>
        <div style={{ marginBottom: 24 }}>
          <h2 style={{ fontSize: 24, fontWeight: 900, color: T.text, letterSpacing: '-0.5px', marginBottom: 6 }}>My Playlists</h2>
          <p style={{ fontSize: 13, color: T.muted }}>Your YouTube playlists synced via the pipeline</p>
        </div>

        {/* pipeline status */}
        <div style={{ background: T.greenLo, borderRadius: 12, border: `1px solid ${T.green}33`, padding: '12px 16px', marginBottom: 20, display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{ width: 8, height: 8, borderRadius: '50%', background: T.green, animation: 'pulse 2s infinite', flexShrink: 0 }} />
          <span style={{ fontSize: 13, color: T.muted }}>
            Playlists sync via <b style={{ color: T.text }}>YouTube API + Dataflow</b> when you create a room
          </span>
        </div>

        {/* playlist cards */}
        {MOCK_PLAYLISTS.map(p => (
          <div key={p.id} className="card-hover" style={{ background: T.card, borderRadius: 14, border: `1px solid ${T.border}`, padding: '14px 18px', marginBottom: 10, display: 'flex', alignItems: 'center', gap: 14, transition: 'all .2s' }}>
            <div style={{ width: 46, height: 46, borderRadius: 10, background: `linear-gradient(135deg,${T.green}33,${T.purple}33)`, flexShrink: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 20 }}>🎵</div>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 14, fontWeight: 700, color: T.text }}>{p.name}</div>
              <div style={{ fontSize: 12, color: T.muted, marginTop: 2 }}>{p.tracks} tracks</div>
            </div>
            <span style={{ padding: '3px 10px', borderRadius: 999, background: T.greenLo, border: `1px solid ${T.green}33`, fontSize: 11, fontWeight: 600, color: T.green }}>{p.genre}</span>
          </div>
        ))}

        <p style={{ textAlign: 'center', fontSize: 12, color: T.dim, marginTop: 16 }}>
          Real playlists load after pipeline integration
        </p>
      </div>
    </div>
  );
}