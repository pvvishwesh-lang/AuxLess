import React, { useState } from 'react';
import { T }                from '../styles/tokens';
import { signInWithGoogle } from '../services/auth';
import Blobs                from '../components/Blobs';

export default function AuthPage({ onLogin, onSkip }) {
  const [loading, setLoading] = useState(false);
  const [error,   setError]   = useState('');

  const handleGoogle = async () => {
    setLoading(true);
    setError('');
    try {
      const userData = await signInWithGoogle();
      onLogin(userData);                  // passes full user object up
    } catch (err) {
      console.error(err);
      if (err.code === 'auth/popup-closed-by-user') {
        setError('Popup closed. Please try again.');
      } else if (err.code === 'auth/popup-blocked') {
        setError('Popup blocked by browser. Allow popups and retry.');
      } else {
        setError('Sign in failed. Please try again.');
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{
      minHeight: '100vh', background: T.bg, color: T.text,
      fontFamily: 'system-ui, sans-serif',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      position: 'relative', padding: '24px 16px',
    }}>
      <Blobs />

      <div className="fade-up" style={{ width: '100%', maxWidth: 400, position: 'relative', zIndex: 1 }}>

        {/* logo */}
        <div style={{ textAlign: 'center', marginBottom: 28 }}>
          <div style={{
            width: 64, height: 64, borderRadius: 20, margin: '0 auto 14px',
            background: `linear-gradient(135deg,${T.green},${T.purple})`,
            display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 30,
          }}>♫</div>
          <h1 style={{ fontSize: 26, fontWeight: 900, letterSpacing: '-0.8px', marginBottom: 6 }}>
            Welcome to AuxLess
          </h1>
          <p style={{ fontSize: 14, color: T.muted, lineHeight: 1.65 }}>
            Sign in so ML can sync your playlists<br />and find what the party wants
          </p>
        </div>

        {/* card */}
        <div style={{
          background: T.card, borderRadius: 22,
          border: `1px solid ${T.border}`, padding: '28px 22px',
        }}>

          {/* error message */}
          {error && (
            <div style={{
              padding: '10px 14px', borderRadius: 10, marginBottom: 14,
              background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.25)',
              color: '#EF4444', fontSize: 13,
            }}>{error}</div>
          )}

          {/* Google button */}
          <button
            onClick={handleGoogle}
            disabled={loading}
            style={{
              width: '100%', padding: '14px 20px',
              display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 12,
              background: loading ? '#f0f0f0' : '#fff',
              borderRadius: 12, border: 'none',
              cursor: loading ? 'not-allowed' : 'pointer',
              fontSize: 15, fontWeight: 600, color: '#1a1a1a',
              transition: 'all .2s', marginBottom: 14,
              boxShadow: '0 2px 8px rgba(0,0,0,0.2)',
            }}
            onMouseEnter={e => !loading && (e.currentTarget.style.opacity = '.9')}
            onMouseLeave={e => !loading && (e.currentTarget.style.opacity = '1')}
          >
            {loading ? (
              <span style={{
                display: 'inline-block', width: 20, height: 20,
                border: '2.5px solid #ddd', borderTopColor: '#1DB954',
                borderRadius: '50%', animation: 'spin .7s linear infinite',
              }} />
            ) : (
              /* real Google G logo SVG */
              <svg width="20" height="20" viewBox="0 0 48 48">
                <path fill="#FFC107" d="M43.6 20H24v8h11.3C33.7 33.6 29.3 36 24 36c-6.6 0-12-5.4-12-12s5.4-12 12-12c3 0 5.8 1.1 7.9 3l5.7-5.7C34.1 6.5 29.3 4 24 4 12.9 4 4 12.9 4 24s8.9 20 20 20c11 0 19.7-8 19.7-20 0-1.3-.1-2.7-.1-4z"/>
                <path fill="#FF3D00" d="M6.3 14.7l6.6 4.8C14.6 15.1 19 12 24 12c3 0 5.8 1.1 7.9 3l5.7-5.7C34.1 6.5 29.3 4 24 4c-7.7 0-14.4 4.2-17.7 10.7z"/>
                <path fill="#4CAF50" d="M24 44c5.2 0 9.9-1.9 13.5-5l-6.2-5.2C29.5 35.5 26.9 36 24 36c-5.3 0-9.7-3.4-11.3-8.1l-6.6 5C9.6 39.8 16.3 44 24 44z"/>
                <path fill="#1976D2" d="M43.6 20H24v8h11.3c-.8 2.4-2.4 4.4-4.4 5.8l6.2 5.2C41.1 35.6 44 30.2 44 24c0-1.3-.1-2.7-.4-4z"/>
              </svg>
            )}
            {loading ? 'Opening Google…' : 'Continue with Google'}
          </button>

          <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 14 }}>
            <div style={{ flex: 1, height: 1, background: T.border }} />
            <span style={{ fontSize: 12, color: T.muted }}>or</span>
            <div style={{ flex: 1, height: 1, background: T.border }} />
          </div>

          {/* guest */}
          <button onClick={onSkip} style={{
            width: '100%', padding: '12px', background: 'transparent',
            borderRadius: 12, border: `1px solid ${T.border}`,
            color: T.muted, fontSize: 14, fontWeight: 500,
            cursor: 'pointer', transition: 'all .2s', marginBottom: 20,
          }}
            onMouseEnter={e => { e.currentTarget.style.borderColor = T.borderH; e.currentTarget.style.color = T.text; }}
            onMouseLeave={e => { e.currentTarget.style.borderColor = T.border;  e.currentTarget.style.color = T.muted; }}
          >Continue as guest (limited)</button>

          {/* feature list */}
          {[
            { icon: '🎵', text: 'Sync your YouTube playlists' },
            { icon: '🧠', text: 'ML clusters your taste in real-time' },
            { icon: '👥', text: 'Create & host party rooms' },
            { icon: '📊', text: 'Live BigQuery analytics' },
          ].map(f => (
            <div key={f.text} style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10 }}>
              <span style={{
                width: 30, height: 30, borderRadius: 8, flexShrink: 0,
                background: T.greenLo, border: `1px solid ${T.green}33`,
                display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 14,
              }}>{f.icon}</span>
              <span style={{ fontSize: 13, color: T.muted }}>{f.text}</span>
            </div>
          ))}
        </div>

        <p style={{ textAlign: 'center', fontSize: 11, color: T.dim, marginTop: 16, lineHeight: 1.6 }}>
          We only access YouTube playlist metadata.<br />
          JWT token stored locally. Never shared.
        </p>
      </div>
    </div>
  );
}