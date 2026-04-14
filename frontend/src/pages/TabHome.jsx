import React, { useState, useEffect } from 'react';
import { T } from '../styles/tokens';
import Blobs  from '../components/Blobs';
import Btn    from '../components/Btn';

const FEATURES = [
  { icon: '🎵', title: 'Genre clustering',  body: 'ML groups your collective taste into dominant clusters in real-time.',  color: '#1DB954' },
  { icon: '👥', title: 'Multi-user rooms',  body: 'Every guest syncs via Firestore. No lag, no waiting.',                  color: '#7C3AED' },
  { icon: '📊', title: 'Live analytics',    body: 'BigQuery + Dataflow pipeline surfaces what the room is feeling.',        color: '#EC4899' },
  { icon: '👍', title: 'Feedback loop',     body: 'Likes and skips instantly retrain the recommendation weights.',          color: '#F59E0B' },
];

function MusicViz() {
  const bars = [3,7,5,9,6,4,8,5,7,3,6,9,4,7,5,8,3,6,9,5,7,4,8,6];
  return (
    <div style={{ display:'flex', alignItems:'flex-end', gap:3, height:48, justifyContent:'center' }}>
      <style>{`
        @keyframes vbar { 0%,100%{transform:scaleY(.2)} 50%{transform:scaleY(1)} }
      `}</style>
      {bars.map((h,i) => (
        <div key={i} style={{
          width: 4, height: h * 5, borderRadius: 3,
          background: i % 3 === 0 ? T.green : i % 3 === 1 ? T.purple : T.pink,
          transformOrigin: 'bottom',
          animation: `vbar ${0.6 + i * 0.08}s ease-in-out ${i * 0.05}s infinite`,
          opacity: 0.7,
        }}/>
      ))}
    </div>
  );
}

function StatPill({ n, label }) {
  return (
    <div style={{ textAlign:'center' }}>
      <div style={{ fontSize:22, fontWeight:900, color:T.text, letterSpacing:'-1px' }}>{n}</div>
      <div style={{ fontSize:11, color:T.muted, marginTop:2 }}>{label}</div>
    </div>
  );
}

export default function TabHome({ onLogin, onTab }) {
  const [activeFeature, setActiveFeature] = useState(0);

  useEffect(() => {
    const id = setInterval(() => setActiveFeature(p => (p+1) % FEATURES.length), 3000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="fade-up" style={{ fontFamily:'system-ui,sans-serif', color:T.text }}>
      <Blobs />

      <div style={{ maxWidth:680, margin:'0 auto', padding:'64px 24px 0', textAlign:'center', position:'relative', zIndex:1 }}>
        <div style={{ position:'relative', zIndex:1 }}>
          <div style={{
            display:'inline-flex', alignItems:'center', gap:8, padding:'5px 14px',
            borderRadius:999, background:T.greenLo, border:`1px solid ${T.green}44`,
            marginBottom:24, fontSize:11, fontWeight:700, color:T.green,
            letterSpacing:'.08em', textTransform:'uppercase',
          }}>
            <span style={{ width:6, height:6, borderRadius:'50%', background:T.green, animation:'pulse 2s infinite' }}/>
            Real-time group recommendations
          </div>

          <h1 style={{
            fontSize:'clamp(38px,7.5vw,68px)', fontWeight:900,
            lineHeight:1.05, letterSpacing:'-2.5px', marginBottom:20,
          }}>
            The aux for{' '}
            <span style={{ background:`linear-gradient(110deg,${T.green} 10%,${T.purple} 70%)`, WebkitBackgroundClip:'text', WebkitTextFillColor:'transparent' }}>
              everyone
            </span>
            {' '}at the party
          </h1>

          <div style={{ marginBottom:20 }}>
            <MusicViz />
          </div>

          <p style={{ fontSize:16, color:T.muted, lineHeight:1.78, marginBottom:36, maxWidth:440, marginLeft:'auto', marginRight:'auto' }}>
            AuxLess syncs every guest's YouTube playlists, clusters your collective taste with ML, and plays exactly what the room wants — live.
          </p>

          <div style={{ display:'flex', gap:12, justifyContent:'center', flexWrap:'wrap', marginBottom:48 }}>
            <Btn onClick={onLogin} style={{ padding:'13px 34px', fontSize:15, borderRadius:14 }}>
              Continue with Google →
            </Btn>
            <Btn variant="outline" onClick={() => onTab('join')} style={{ padding:'13px 26px', fontSize:15, borderRadius:14 }}>
              Join a room
            </Btn>
          </div>

          <div style={{
            display:'flex', gap:32, justifyContent:'center',
            padding:'20px 0', marginBottom:48,
            borderTop:`1px solid ${T.border}`, borderBottom:`1px solid ${T.border}`,
          }}>
            <StatPill n="2.4k" label="Active rooms" />
            <StatPill n="18k"  label="Songs played today" />
            <StatPill n="99ms" label="Avg latency" />
            <StatPill n="60s"  label="ML window" />
          </div>
        </div>
      </div>

      <div style={{ maxWidth:880, margin:'0 auto', padding:'0 24px', position:'relative', zIndex:1 }}>
        <div style={{ textAlign:'center', marginBottom:24 }}>
          <h2 style={{ fontSize:22, fontWeight:800, letterSpacing:'-0.5px', color:T.text, marginBottom:8 }}>
            Built different
          </h2>
          <p style={{ fontSize:14, color:T.muted }}>Every feature talks to the pipeline in real-time</p>
        </div>

        <div style={{ display:'flex', gap:8, justifyContent:'center', marginBottom:20, flexWrap:'wrap' }}>
          {FEATURES.map((f,i) => (
            <button key={f.title} onClick={() => setActiveFeature(i)} style={{
              padding:'8px 16px', borderRadius:999, fontSize:13, fontWeight:600,
              cursor:'pointer', transition:'all .2s',
              border:`1.5px solid ${activeFeature===i ? f.color : 'rgba(255,255,255,0.09)'}`,
              background: activeFeature===i ? `${f.color}18` : 'transparent',
              color: activeFeature===i ? f.color : T.muted,
            }}>
              {f.icon} {f.title}
            </button>
          ))}
        </div>

        <div style={{
          background:T.card, borderRadius:18, border:`1px solid ${FEATURES[activeFeature].color}33`,
          padding:'28px 28px', marginBottom:20, transition:'all .3s',
          display:'flex', alignItems:'center', gap:20,
        }}>
          <div style={{
            width:60, height:60, borderRadius:16, flexShrink:0,
            background:`${FEATURES[activeFeature].color}18`,
            display:'flex', alignItems:'center', justifyContent:'center', fontSize:28,
          }}>{FEATURES[activeFeature].icon}</div>
          <div>
            <div style={{ fontWeight:800, fontSize:18, color:T.text, marginBottom:6 }}>{FEATURES[activeFeature].title}</div>
            <div style={{ fontSize:14, color:T.muted, lineHeight:1.65 }}>{FEATURES[activeFeature].body}</div>
          </div>
        </div>

        <div style={{ display:'flex', gap:6, justifyContent:'center', marginBottom:48 }}>
          {FEATURES.map((_,i) => (
            <div key={i} onClick={() => setActiveFeature(i)} style={{
              width: activeFeature===i ? 20 : 6, height:6, borderRadius:3,
              background: activeFeature===i ? T.green : 'rgba(255,255,255,0.15)',
              cursor:'pointer', transition:'all .3s',
            }}/>
          ))}
        </div>
      </div>

      <footer style={{
        maxWidth:880, margin:'48px auto 0', padding:'32px 24px 24px',
        borderTop:`1px solid ${T.border}`, position:'relative', zIndex:1,
      }}>
        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', flexWrap:'wrap', gap:24, marginBottom:28 }}>
          <div style={{ maxWidth:220 }}>
            <div style={{ display:'flex', alignItems:'center', gap:9, marginBottom:10 }}>
              <div style={{ width:28, height:28, borderRadius:8, background:`linear-gradient(135deg,${T.green},${T.purple})`, display:'flex', alignItems:'center', justifyContent:'center', fontSize:14 }}>♫</div>
              <span style={{ fontWeight:900, fontSize:16, color:T.text }}>AuxLess</span>
            </div>
            <p style={{ fontSize:12, color:T.muted, lineHeight:1.7 }}>
              Party music recommendation system. ML-powered, real-time, built for the room.
            </p>
          </div>
          <div style={{ display:'flex', gap:48, flexWrap:'wrap' }}>
            <div>
              <div style={{ fontSize:11, fontWeight:700, color:T.muted, textTransform:'uppercase', letterSpacing:'.1em', marginBottom:12 }}>Product</div>
              {['Home','Create room','Join room','Analytics'].map(l => (
                <div key={l} style={{ fontSize:13, color:T.muted, marginBottom:8, cursor:'pointer' }}
                  onMouseEnter={e=>e.target.style.color=T.text}
                  onMouseLeave={e=>e.target.style.color=T.muted}
                >{l}</div>
              ))}
            </div>
            <div>
              <div style={{ fontSize:11, fontWeight:700, color:T.muted, textTransform:'uppercase', letterSpacing:'.1em', marginBottom:12 }}>Tech Stack</div>
              {['Apache Beam','Dataflow','BigQuery','Firestore','Cloud Run'].map(l => (
                <div key={l} style={{ fontSize:13, color:T.muted, marginBottom:8 }}>{l}</div>
              ))}
            </div>
            <div>
              <div style={{ fontSize:11, fontWeight:700, color:T.muted, textTransform:'uppercase', letterSpacing:'.1em', marginBottom:12 }}>Team</div>
              {['GitHub','Docs','Pipeline','ML Model'].map(l => (
                <div key={l} style={{ fontSize:13, color:T.muted, marginBottom:8, cursor:'pointer' }}
                  onMouseEnter={e=>e.target.style.color=T.text}
                  onMouseLeave={e=>e.target.style.color=T.muted}
                >{l}</div>
              ))}
            </div>
          </div>
        </div>
        <div style={{
          display:'flex', justifyContent:'space-between', alignItems:'center',
          paddingTop:20, borderTop:`1px solid ${T.border}`, flexWrap:'wrap', gap:12,
        }}>
          <span style={{ fontSize:12, color:T.dim }}>© 2026 AuxLess · Built for Google Demo · MIT License</span>
          <div style={{ display:'flex', gap:16 }}>
            {['Privacy','Terms','Contact'].map(l => (
              <span key={l} style={{ fontSize:12, color:T.dim, cursor:'pointer' }}
                onMouseEnter={e=>e.target.style.color=T.muted}
                onMouseLeave={e=>e.target.style.color=T.dim}
              >{l}</span>
            ))}
          </div>
        </div>
      </footer>
    </div>
  );
}