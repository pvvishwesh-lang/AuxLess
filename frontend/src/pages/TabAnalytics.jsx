import React, { useState, useEffect } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid, Cell, LineChart, Line
} from 'recharts';
import { T } from '../styles/tokens';
import { mlDb } from '../config/firebase';
import { collection, onSnapshot } from 'firebase/firestore';
import {
  MOCK_BIAS_METRICS, PIPELINE_STEPS
} from '../data/mockData';

const toSessionId = (id) => (id || '').replace(/^AUX-/i, '').toLowerCase();

const CustomTip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: T.card, border: `1px solid ${T.border}`, borderRadius: 10, padding: '10px 14px', fontSize: 12 }}>
      <p style={{ color: T.muted, marginBottom: 4 }}>{label}</p>
      {payload.map((p, i) => (
        <p key={i} style={{ color: p.color, fontWeight: 600 }}>{p.name}: {typeof p.value === 'number' ? p.value.toFixed(2) : p.value}</p>
      ))}
    </div>
  );
};

function StatCard({ n, label, color, sub }) {
  return (
    <div style={{ background: T.card, borderRadius: 14, border: `1px solid ${T.border}`, padding: '18px 16px', textAlign: 'center' }}>
      <div style={{ fontSize: 26, fontWeight: 800, color, letterSpacing: '-1px' }}>{n}</div>
      <div style={{ fontSize: 12, color: T.muted, marginTop: 4 }}>{label}</div>
      {sub && <div style={{ fontSize: 11, color, marginTop: 3, fontWeight: 600 }}>{sub}</div>}
    </div>
  );
}

export default function TabAnalytics({ roomId }) {
  const [biasSlice, setBiasSlice] = useState('genre');
  const [windowCount, setWindowCount] = useState(5);
  const [recommendations, setRecommendations] = useState([]);
  const [feedbackEvents, setFeedbackEvents] = useState([]);
  const [totalEvents, setTotalEvents] = useState(0);

  const sessionId = toSessionId(roomId);

  // Live recommendations from Firestore
  useEffect(() => {
    if (!sessionId) return;
    const unsub = onSnapshot(
      collection(mlDb, 'sessions', sessionId, 'recommendations'),
      (snap) => {
        if (snap.empty) return;
        setRecommendations(snap.docs.map(d => ({
          id: d.id,
          name: (d.data().track_title || d.data().title || 'Unknown').slice(0, 12),
          score: parseFloat(d.data().final_score || 0),
          likes: d.data().like_count || 0,
          dislikes: d.data().dislike_count || 0,
          genre: d.data().genre || '',
        })));
      }, () => {}
    );
    return () => unsub();
  }, [sessionId]);

  // Live feedback events from Firestore
  useEffect(() => {
    if (!sessionId) return;
    const unsub = onSnapshot(
      collection(mlDb, 'sessions', sessionId, 'feedback_events'),
      (snap) => {
        setTotalEvents(snap.size);
        setFeedbackEvents(snap.docs.map(d => d.data()));
      }, () => {}
    );
    return () => unsub();
  }, [sessionId]);

  // Window counter
  useEffect(() => {
    const id = setInterval(() => setWindowCount(w => w + 1), 60000);
    return () => clearInterval(id);
  }, []);

  // Build chart data from live recommendations
  const scoreData = recommendations.map(r => ({
    name: r.name,
    Score: r.score,
    color: r.score >= 0 ? T.green : '#EF4444',
  }));

  const likesData = recommendations.map(r => ({
    name: r.name,
    '👍 Likes': r.likes,
    '👎 Dislikes': r.dislikes,
  }));

  // Count event types
  const eventCounts = feedbackEvents.reduce((acc, e) => {
    acc[e.event_type] = (acc[e.event_type] || 0) + 1;
    return acc;
  }, {});

  const eventData = Object.entries(eventCounts).map(([type, count]) => ({
    name: type,
    count,
    color: type === 'like' ? T.green : type === 'dislike' ? '#EF4444' : type === 'play' ? T.purple : T.amber,
  }));

  const avgScore = recommendations.length
    ? (recommendations.reduce((a, r) => a + r.score, 0) / recommendations.length).toFixed(2)
    : '0.00';

  return (
    <div className="fade-up" style={{ maxWidth: 860, margin: '0 auto', padding: '36px 20px 80px' }}>

      {/* header */}
      <div style={{ marginBottom: 24 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 }}>
          <h2 style={{ fontSize: 22, fontWeight: 900, letterSpacing: '-0.6px', color: T.text }}>
            Live Analytics
          </h2>
          <span style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '3px 10px', borderRadius: 999, background: T.greenLo, border: `1px solid ${T.green}44`, fontSize: 11, fontWeight: 700, color: T.green }}>
            <span style={{ width: 6, height: 6, borderRadius: '50%', background: T.green, animation: 'pulse 2s infinite' }} />
            LIVE · window #{windowCount}
          </span>
        </div>
        <p style={{ fontSize: 13, color: T.muted }}>
          Streaming pipeline: Pub/Sub → ParseEventFn → AggregateMetricsFn → BigQuery · 60s fixed windows
        </p>
      </div>

      {/* stat cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(130px,1fr))', gap: 10, marginBottom: 20 }}>
        <StatCard n={totalEvents} label="Events processed" color={T.green} sub="live" />
        <StatCard n={recommendations.length} label="ML recommendations" color={T.purple} />
        <StatCard n={avgScore} label="Avg ML score" color={T.pink} />
        <StatCard n={windowCount} label="Windows processed" color={T.amber} sub="60s each" />
      </div>

      {/* feedback scores — live from ML */}
      {scoreData.length > 0 && (
        <div style={{ background: T.card, borderRadius: 18, border: `1px solid ${T.border}`, padding: '20px 18px', marginBottom: 14 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: T.muted, textTransform: 'uppercase', letterSpacing: '.09em', marginBottom: 4 }}>
            ML Recommendation Scores — Live
          </div>
          <div style={{ fontSize: 11, color: T.dim, marginBottom: 14 }}>
            final_score from ML model · updates in real-time
          </div>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={scoreData} margin={{ top: 4, right: 8, bottom: 0, left: -20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false} />
              <XAxis dataKey="name" tick={{ fill: T.muted, fontSize: 11 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: T.muted, fontSize: 10 }} axisLine={false} tickLine={false} />
              <Tooltip content={<CustomTip />} />
              <Bar dataKey="Score" radius={[6, 6, 0, 0]} maxBarSize={48}>
                {scoreData.map((d, i) => (
                  <Cell key={i} fill={d.color} fillOpacity={0.85} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* likes vs dislikes */}
      {likesData.length > 0 && (
        <div style={{ background: T.card, borderRadius: 18, border: `1px solid ${T.border}`, padding: '20px 18px', marginBottom: 14 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: T.muted, textTransform: 'uppercase', letterSpacing: '.09em', marginBottom: 4 }}>
            Likes vs Dislikes — Live Voting
          </div>
          <ResponsiveContainer width="100%" height={180}>
            <BarChart data={likesData} margin={{ top: 4, right: 8, bottom: 0, left: -20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false} />
              <XAxis dataKey="name" tick={{ fill: T.muted, fontSize: 11 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: T.muted, fontSize: 10 }} axisLine={false} tickLine={false} />
              <Tooltip content={<CustomTip />} />
              <Bar dataKey="👍 Likes" fill={T.green} radius={[6, 6, 0, 0]} maxBarSize={32} />
              <Bar dataKey="👎 Dislikes" fill="#EF4444" radius={[6, 6, 0, 0]} maxBarSize={32} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* event types */}
      {eventData.length > 0 && (
        <div style={{ background: T.card, borderRadius: 18, border: `1px solid ${T.border}`, padding: '20px 18px', marginBottom: 14 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: T.muted, textTransform: 'uppercase', letterSpacing: '.09em', marginBottom: 4 }}>
            Pipeline Events — Live
          </div>
          <div style={{ fontSize: 11, color: T.dim, marginBottom: 14 }}>
            play · like · dislike · skip · complete · replay
          </div>
          <ResponsiveContainer width="100%" height={180}>
            <BarChart data={eventData} margin={{ top: 4, right: 8, bottom: 0, left: -20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false} />
              <XAxis dataKey="name" tick={{ fill: T.muted, fontSize: 11 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: T.muted, fontSize: 10 }} axisLine={false} tickLine={false} />
              <Tooltip content={<CustomTip />} />
              <Bar dataKey="count" radius={[6, 6, 0, 0]} maxBarSize={48}>
                {eventData.map((d, i) => (
                  <Cell key={i} fill={d.color} fillOpacity={0.85} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* bias metrics */}
      <div style={{ background: T.card, borderRadius: 18, border: `1px solid ${T.border}`, padding: '20px 20px', marginBottom: 14 }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16, flexWrap: 'wrap', gap: 8 }}>
          <div>
            <div style={{ fontSize: 12, fontWeight: 700, color: T.muted, textTransform: 'uppercase', letterSpacing: '.09em' }}>
              Bias metrics — run_bias_mitigation
            </div>
            <div style={{ fontSize: 11, color: T.dim, marginTop: 3 }}>
              Slices &lt;5% upsampled · slices &gt;60% downsampled
            </div>
          </div>
          <div style={{ display: 'flex', background: T.surface, borderRadius: 8, padding: 3, border: `1px solid ${T.border}` }}>
            {['genre', 'country'].map(s => (
              <button key={s} onClick={() => setBiasSlice(s)} style={{
                padding: '5px 14px', borderRadius: 6, fontSize: 12, fontWeight: 600,
                border: 'none', cursor: 'pointer',
                background: biasSlice === s ? T.card : 'transparent',
                color: biasSlice === s ? T.text : T.muted,
                transition: 'all .18s', textTransform: 'capitalize',
              }}>{s}</button>
            ))}
          </div>
        </div>
        {MOCK_BIAS_METRICS[biasSlice].map(b => (
          <div key={b.slice} style={{ marginBottom: 10 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4, alignItems: 'center' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ fontSize: 12, color: T.muted }}>{b.slice}</span>
                {b.adjusted && (
                  <span style={{ fontSize: 10, fontWeight: 700, padding: '1px 6px', borderRadius: 4, background: `${T.amber}18`, color: T.amber, border: `1px solid ${T.amber}33` }}>ADJUSTED</span>
                )}
              </div>
              <span style={{ fontSize: 12, fontWeight: 600, color: T.text }}>{b.pct}%</span>
            </div>
            <div style={{ background: 'rgba(255,255,255,0.06)', borderRadius: 6, height: 7, overflow: 'hidden' }}>
              <div style={{ width: `${b.pct}%`, height: 7, borderRadius: 6, background: b.color, transition: 'width 1.2s cubic-bezier(.4,0,.2,1)' }} />
            </div>
          </div>
        ))}
      </div>

      {/* pipeline steps */}
      <div style={{ background: T.card, borderRadius: 18, border: `1px solid ${T.border}`, padding: '20px 20px' }}>
        <div style={{ fontSize: 12, fontWeight: 700, color: T.muted, textTransform: 'uppercase', letterSpacing: '.09em', marginBottom: 16 }}>
          Pipeline status — run_for_session
        </div>
        {PIPELINE_STEPS.map((p, i) => (
          <div key={p.step} style={{
            display: 'flex', gap: 14, alignItems: 'flex-start',
            padding: '11px 0',
            borderBottom: i < PIPELINE_STEPS.length - 1 ? `1px solid ${T.border}` : 'none',
          }}>
            <div style={{ width: 8, height: 8, borderRadius: '50%', background: p.color, marginTop: 5, flexShrink: 0, animation: p.status !== 'done' ? 'pulse 2s infinite' : 'none' }} />
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontSize: 13, fontWeight: 700, color: T.text }}>{p.step}</div>
              <div style={{ fontSize: 11, color: T.muted, marginTop: 2, lineHeight: 1.5 }}>{p.detail}</div>
            </div>
            <span style={{ padding: '3px 10px', borderRadius: 999, fontSize: 11, fontWeight: 700, background: `${p.color}18`, color: p.color, flexShrink: 0 }}>{p.status}</span>
          </div>
        ))}
      </div>
    </div>
  );
}