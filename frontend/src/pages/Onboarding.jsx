import React, { useState } from 'react';
import { T } from '../styles/tokens';
import { GENRES, ARTISTS } from '../data/mockData';
import Btn from '../components/Btn';
import ArtistImage from '../components/ArtistImage';

const GENRE_COLORS = {
  'Hip-Hop':'#1DB954','Pop':'#7C3AED','R&B':'#EC4899','Electronic':'#3B82F6',
  'Rock':'#EF4444','Latin':'#F59E0B','Afrobeats':'#10B981','Indie':'#8B5CF6',
  'K-Pop':'#F472B6','House':'#06B6D4','Drill':'#6366F1','Reggaeton':'#F97316',
  'Jazz':'#84CC16','Soul':'#E879F9',
};

const GENRE_EMOJI = {
  'Hip-Hop':'🎤','Pop':'🌟','R&B':'🎷','Electronic':'🎧','Rock':'🎸',
  'Latin':'💃','Afrobeats':'🥁','Indie':'🎻','K-Pop':'✨','House':'🎹',
  'Drill':'🔥','Reggaeton':'🌴','Jazz':'🎺','Soul':'❤️',
};

const getColor = (name) => {
  const colors = ['#1DB954','#7C3AED','#EC4899','#F59E0B','#3B82F6','#10B981','#EF4444','#8B5CF6'];
  let hash = 0;
  for (let i = 0; i < name.length; i++) hash = name.charCodeAt(i) + ((hash << 5) - hash);
  return colors[Math.abs(hash) % colors.length];
};

export default function Onboarding({ onDone }) {
  const [step,    setStep]    = useState(0);
  const [genres,  setGenres]  = useState([]);
  const [artists, setArtists] = useState([]);

  const tg = g => setGenres(p  => p.includes(g) ? p.filter(x => x !== g) : [...p, g]);
  const ta = a => setArtists(p => p.includes(a) ? p.filter(x => x !== a) : [...p, a]);

  const STEPS = [
    // ── STEP 1: GENRES ──────────────────────────────────────
    {
      title: 'What genres move you?',
      sub:   'Pick at least 2 - shapes your room vibe',
      ok:    genres.length >= 2,
      el: (
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(110px, 1fr))',
          gap: 10, width: '100%',
        }}>
          {GENRES.map(g => {
            const on    = genres.includes(g);
            const color = GENRE_COLORS[g] || T.green;
            const emoji = GENRE_EMOJI[g]  || '🎵';
            return (
              <button key={g} onClick={() => tg(g)} style={{
                padding: '12px 8px', borderRadius: 14,
                border: `1.5px solid ${on ? color : 'rgba(255,255,255,0.09)'}`,
                background: on ? `${color}20` : 'rgba(255,255,255,0.03)',
                cursor: 'pointer', transition: 'all .18s',
                display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 7,
              }}>
                <div style={{
                  width: 40, height: 40, borderRadius: 10,
                  background: on ? `${color}30` : 'rgba(255,255,255,0.06)',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  fontSize: 20, border: on ? `1px solid ${color}44` : '1px solid transparent',
                  transition: 'all .18s',
                }}>
                  {on ? '✓' : emoji}
                </div>
                <span style={{
                  fontSize: 12, fontWeight: 600,
                  color: on ? color : T.muted,
                  textAlign: 'center',
                }}>{g}</span>
              </button>
            );
          })}
        </div>
      ),
    },

    // ── STEP 2: ARTISTS with REAL iTunes images ──────────────
    {
      title: 'Your favourite artists?',
      sub:   'Select at least 1 - seeds the ML genre cluster',
      ok:    artists.length >= 1,
      el: (
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(95px, 1fr))',
          gap: 12, width: '100%',
        }}>
          {ARTISTS.map(a => {
            const on    = artists.includes(a);
            const color = getColor(a);
            return (
              <button key={a} onClick={() => ta(a)} style={{
                padding: '14px 8px 12px', borderRadius: 16,
                border: `1.5px solid ${on ? color : 'rgba(255,255,255,0.08)'}`,
                background: on ? `${color}18` : 'rgba(255,255,255,0.03)',
                cursor: 'pointer', transition: 'all .22s',
                display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 9,
                position: 'relative',
              }}>
                {/* green checkmark badge when selected */}
                {on && (
                  <div style={{
                    position: 'absolute', top: 6, right: 6,
                    width: 18, height: 18, borderRadius: '50%',
                    background: color,
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    fontSize: 10, color: '#fff', fontWeight: 800, zIndex: 2,
                  }}>✓</div>
                )}

                {/* REAL iTunes artist image via ArtistImage component */}
                <ArtistImage
                  name={a}
                  size={60}
                  radius={30}
                  style={{
                    border: `2.5px solid ${on ? color : 'rgba(255,255,255,0.1)'}`,
                    transition: 'border-color .22s',
                  }}
                />

                {/* artist name */}
                <span style={{
                  fontSize: 11, fontWeight: 600,
                  color: on ? color : T.muted,
                  textAlign: 'center', lineHeight: 1.3,
                  maxWidth: '100%',
                }}>{a}</span>
              </button>
            );
          })}
        </div>
      ),
    },

    // ── STEP 3: CONFIRM ──────────────────────────────────────
    {
      title: "You're all set 🎉",
      sub:   'AuxLess will sync your taste with the room in real-time',
      ok:    true,
      el: (
        <div style={{ textAlign: 'center', width: '100%' }}>
          <div style={{ fontSize: 52, marginBottom: 16 }}>🎉</div>

          {/* selected genres */}
          <div style={{ display: 'flex', gap: 7, justifyContent: 'center', flexWrap: 'wrap', marginBottom: 14 }}>
            {genres.map(g => (
              <span key={g} style={{
                padding: '4px 12px', borderRadius: 999, fontSize: 12, fontWeight: 600,
                background: `${GENRE_COLORS[g] || T.green}18`,
                border: `1px solid ${GENRE_COLORS[g] || T.green}44`,
                color: GENRE_COLORS[g] || T.green,
              }}>
                {GENRE_EMOJI[g]} {g}
              </span>
            ))}
          </div>

          {/* selected artists with their real images */}
          <div style={{ display: 'flex', gap: 8, justifyContent: 'center', flexWrap: 'wrap' }}>
            {artists.map(a => {
              const color = getColor(a);
              return (
                <div key={a} style={{
                  display: 'flex', alignItems: 'center', gap: 7,
                  padding: '4px 12px 4px 4px', borderRadius: 999,
                  background: `${color}15`, border: `1px solid ${color}33`,
                }}>
                  <ArtistImage name={a} size={22} radius={11} />
                  <span style={{ fontSize: 12, fontWeight: 600, color }}>{a}</span>
                </div>
              );
            })}
          </div>

          <p style={{ color: T.muted, fontSize: 13, lineHeight: 1.7, marginTop: 16, maxWidth: 300, margin: '16px auto 0' }}>
            The ML pipeline will use this to seed<br />genre clusters when you join a room.
          </p>
        </div>
      ),
    },
  ];

  const cur = STEPS[step];

  return (
    <div style={{
      position: 'fixed', inset: 0,
      background: 'rgba(0,0,0,.8)', backdropFilter: 'blur(10px)',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      zIndex: 100, padding: 16, overflowY: 'auto',
    }}>
      <div className="fade-up" style={{
        background: T.card, borderRadius: 24,
        border: `1px solid ${T.border}`,
        padding: '32px 28px 26px',
        maxWidth: 540, width: '100%',
        maxHeight: '90vh', overflowY: 'auto',
        margin: 'auto',
      }}>

        {/* progress dots */}
        <div style={{ display: 'flex', gap: 6, justifyContent: 'center', marginBottom: 26 }}>
          {STEPS.map((_, i) => (
            <div key={i} style={{
              height: 5, borderRadius: 3,
              width: i === step ? 32 : 8,
              background: i <= step ? T.green : 'rgba(255,255,255,0.1)',
              transition: 'all .3s',
            }} />
          ))}
        </div>

        {/* header */}
        <div style={{ textAlign: 'center', marginBottom: 22 }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: T.green, textTransform: 'uppercase', letterSpacing: '.1em', marginBottom: 8 }}>
            Step {step + 1} of {STEPS.length}
          </div>
          <h2 style={{ fontSize: 22, fontWeight: 900, letterSpacing: '-0.5px', color: T.text, marginBottom: 6 }}>
            {cur.title}
          </h2>
          <p style={{ color: T.muted, fontSize: 13 }}>{cur.sub}</p>
        </div>

        {/* content */}
        <div style={{ marginBottom: 18 }}>{cur.el}</div>

        {/* selection count */}
        {step === 0 && genres.length > 0 && (
          <p style={{ textAlign: 'center', fontSize: 12, color: T.green, marginBottom: 12 }}>
            {genres.length} genre{genres.length > 1 ? 's' : ''} selected ✓
          </p>
        )}
        {step === 1 && artists.length > 0 && (
          <p style={{ textAlign: 'center', fontSize: 12, color: T.purple, marginBottom: 12 }}>
            {artists.length} artist{artists.length > 1 ? 's' : ''} selected ✓
          </p>
        )}

        {/* buttons */}
        <div style={{ display: 'flex', gap: 10 }}>
          {step > 0 && (
            <Btn variant="outline" onClick={() => setStep(s => s - 1)} full>← Back</Btn>
          )}
          <Btn
            onClick={step === STEPS.length - 1 ? () => onDone(genres, artists) : () => setStep(s => s + 1)}
            disabled={!cur.ok}
            full
          >
            {step === STEPS.length - 1 ? 'Start the party 🎉' : 'Continue →'}
          </Btn>
        </div>
      </div>
    </div>
  );
}