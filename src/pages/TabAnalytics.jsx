import React, { useState, useEffect } from 'react';
import {
  LineChart, Line, BarChart, Bar,
  XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Cell
} from 'recharts';
import { T } from '../styles/tokens';
import {
  MOCK_STREAM_EVENTS,
  MOCK_FEEDBACK_SCORES,
  MOCK_BIAS_METRICS,
  PIPELINE_STEPS,
} from '../data/mockData';

// custom tooltip
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

// stat card
function StatCard({ n, label, color, sub }) {
  return (
    <div style={{ background: T.card, borderRadius: 14, border: `1px solid ${T.border}`, padding: '18px 16px', textAlign: 'center' }}>
      <div style={{ fontSize: 26, fontWeight: 800, color, letterSpacing: '-1px' }}>{n}</div>
      <div style={{ fontSize: 12, color: T.muted, marginTop: 4 }}>{label}</div>
      {sub && <div style={{ fontSize: 11, color, marginTop: 3, fontWeight: 600 }}>{sub}</div>}
    </div>
  );
}

export default function TabAnalytics() {
  const [biasSlice, setBiasSlice] = useState('genre');
  const [, setTick] = useState(0);
  const [liveEvents, setLiveEvents] = useState(MOCK_STREAM_EVENTS);
  const [windowCount, setWindowCount] = useState(5);

  // simulate real-time window updates (matches FixedWindows(60) in streaming_pipeline.py)
  useEffect(() => {
    const id = setInterval(() => {
      setTick(t => t + 1);
      setWindowCount(w => w + 1);
      setLiveEvents(prev => {
        const updated = prev.map(e => ({
          ...e,
          play_count:     e.play_count     + Math.floor(Math.random() * 3),
          skip_count:     e.skip_count     + (Math.random() > 0.7 ? 1 : 0),
          complete_count: e.complete_count + Math.floor(Math.random() * 2),
          total_events:   e.total_events   + Math.floor(Math.random() * 4),
        }));
        return updated.map(e => ({
          ...e,
          completion_rate: e.total_events > 0
            ? parseFloat((e.complete_count / e.total_events).toFixed(4))
            : 0,
        }));
      });
    }, 3000);
    return () => clearInterval(id);
  }, []);

  // completion rate chart data (from AggregateMetricsFn output)
  const completionData = liveEvents.map(e => ({
    name: e.song_name.length > 10 ? e.song_name.slice(0, 10) + '…' : e.song_name,
    'Completion %': parseFloat((e.completion_rate * 100).toFixed(1)),
    'Play count':   e.play_count,
    'Skip count':   e.skip_count,
  }));

  // feedback score chart (from ScoreFeedbackFn — like+2, dislike-2, skip-0.5, replay+1.5)
  const scoreData = MOCK_FEEDBACK_SCORES.map(s => ({
    name:  s.song_name.length > 10 ? s.song_name.slice(0, 10) + '…' : s.song_name,
    Score: s.score,
    color: s.score >= 0 ? T.green : '#EF4444',
  }));

  // total events processed
  const totalEvents = liveEvents.reduce((a, e) => a + e.total_events, 0);
  const avgCompletion = (liveEvents.reduce((a, e) => a + e.completion_rate, 0) / liveEvents.length * 100).toFixed(1);

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
        <StatCard n={totalEvents.toLocaleString()} label="Events processed"   color={T.green}  sub={`+${Math.floor(Math.random()*12)+4}/s`} />
        <StatCard n={`${avgCompletion}%`}           label="Avg completion rate" color={T.purple} />
        <StatCard n={windowCount}                   label="Windows processed"  color={T.pink}   sub="60s each" />
        <StatCard n="99ms"                          label="Pipeline latency"   color={T.amber}  />
      </div>

      {/* completion rate bar chart — AggregateMetricsFn output */}
      <div style={{ background: T.card, borderRadius: 18, border: `1px solid ${T.border}`, padding: '20px 18px', marginBottom: 14 }}>
        <div style={{ fontSize: 12, fontWeight: 700, color: T.muted, textTransform: 'uppercase', letterSpacing: '.09em', marginBottom: 4 }}>
          Completion rate by track — live (AggregateMetricsFn)
        </div>
        <div style={{ fontSize: 11, color: T.dim, marginBottom: 14 }}>
          complete_count / total_events · updates every 60s window
        </div>
        <ResponsiveContainer width="100%" height={200}>
          <BarChart data={completionData} margin={{ top: 4, right: 8, bottom: 0, left: -20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false} />
            <XAxis dataKey="name" tick={{ fill: T.muted, fontSize: 11 }} axisLine={false} tickLine={false} />
            <YAxis tick={{ fill: T.muted, fontSize: 10 }} axisLine={false} tickLine={false} domain={[0, 100]} />
            <Tooltip content={<CustomTip />} />
            <Bar dataKey="Completion %" radius={[6, 6, 0, 0]} maxBarSize={48}>
              {completionData.map((_, i) => (
                <Cell key={i} fill={i === 0 ? T.green : T.purple} fillOpacity={0.85} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* play vs skip line chart */}
      <div style={{ background: T.card, borderRadius: 18, border: `1px solid ${T.border}`, padding: '20px 18px', marginBottom: 14 }}>
        <div style={{ fontSize: 12, fontWeight: 700, color: T.muted, textTransform: 'uppercase', letterSpacing: '.09em', marginBottom: 4 }}>
          Play count vs skip count — live
        </div>
        <div style={{ fontSize: 11, color: T.dim, marginBottom: 14 }}>
          From ParseEventFn → windowed aggregation · actions: play, skip, complete, like, dislike, replay
        </div>
        <ResponsiveContainer width="100%" height={180}>
          <LineChart data={completionData} margin={{ top: 4, right: 8, bottom: 0, left: -20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={T.border} vertical={false} />
            <XAxis dataKey="name" tick={{ fill: T.muted, fontSize: 11 }} axisLine={false} tickLine={false} />
            <YAxis tick={{ fill: T.muted, fontSize: 10 }} axisLine={false} tickLine={false} />
            <Tooltip content={<CustomTip />} />
            <Line type="monotone" dataKey="Play count" stroke={T.green}  strokeWidth={2} dot={{ fill: T.green,  r: 3 }} />
            <Line type="monotone" dataKey="Skip count" stroke="#EF4444"  strokeWidth={2} dot={{ fill: '#EF4444', r: 3 }} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* feedback score chart — ScoreFeedbackFn */}
      <div style={{ background: T.card, borderRadius: 18, border: `1px solid ${T.border}`, padding: '20px 18px', marginBottom: 14 }}>
        <div style={{ fontSize: 12, fontWeight: 700, color: T.muted, textTransform: 'uppercase', letterSpacing: '.09em', marginBottom: 4 }}>
          Feedback scores — ScoreFeedbackFn
        </div>
        <div style={{ fontSize: 11, color: T.dim, marginBottom: 14 }}>
          Weights: like +2.0 · dislike -2.0 · skip -0.5 · replay +1.5 · stored in Firestore sessions/tracks
        </div>
        <ResponsiveContainer width="100%" height={180}>
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

      {/* bias metrics — bias_analyser slices */}
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
          {/* slice toggle */}
          <div style={{ display: 'flex', background: T.surface, borderRadius: 8, padding: 3, border: `1px solid ${T.border}` }}>
            {['genre','country'].map(s => (
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
                  <span style={{ fontSize: 10, fontWeight: 700, padding: '1px 6px', borderRadius: 4, background: `${T.amber}18`, color: T.amber, border: `1px solid ${T.amber}33` }}>
                    ADJUSTED
                  </span>
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

      {/* pipeline steps — run_for_session */}
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
            <div style={{
              width: 8, height: 8, borderRadius: '50%', background: p.color,
              marginTop: 5, flexShrink: 0,
              animation: p.status !== 'done' ? 'pulse 2s infinite' : 'none',
            }} />
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontSize: 13, fontWeight: 700, color: T.text }}>{p.step}</div>
              <div style={{ fontSize: 11, color: T.muted, marginTop: 2, lineHeight: 1.5 }}>{p.detail}</div>
            </div>
            <span style={{
              padding: '3px 10px', borderRadius: 999, fontSize: 11, fontWeight: 700,
              background: `${p.color}18`, color: p.color, flexShrink: 0,
            }}>{p.status}</span>
          </div>
        ))}
      </div>
    </div>
  );
}