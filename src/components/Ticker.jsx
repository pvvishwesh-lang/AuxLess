import React from 'react';
import { GENRES } from '../data/mockData';
import { T } from '../styles/tokens';

export default function Ticker() {
  const items = [...GENRES, ...GENRES];
  return (
    <div style={{
      overflow:'hidden', whiteSpace:'nowrap', padding:'14px 0',
      borderTop:`1px solid ${T.border}`, borderBottom:`1px solid ${T.border}`,
    }}>
      <div style={{ display:'inline-flex', gap:12, animation:'ticker 20s linear infinite' }}>
        {items.map((g, i) => (
          <span key={i} style={{
            padding:'4px 14px', borderRadius:999,
            background: T.surface, border:`1px solid ${T.border}`,
            fontSize:12, color:T.muted, fontWeight:500,
          }}>{g}</span>
        ))}
      </div>
    </div>
  );
}