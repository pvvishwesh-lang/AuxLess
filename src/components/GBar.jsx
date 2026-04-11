import React, { useState, useEffect } from 'react';
import { T } from '../styles/tokens';

export default function GBar({ label, pct, color }) {
  const [w, setW] = useState(0);
  useEffect(() => {
    const t = setTimeout(() => setW(pct), 100);
    return () => clearTimeout(t);
  }, [pct]);

  return (
    <div style={{ marginBottom: 11 }}>
      <div style={{ display:'flex', justifyContent:'space-between', marginBottom:5 }}>
        <span style={{ fontSize:12, color:T.muted }}>{label}</span>
        <span style={{ fontSize:12, fontWeight:600, color:T.text }}>{pct}%</span>
      </div>
      <div style={{ background:'rgba(255,255,255,0.06)', borderRadius:6, height:6, overflow:'hidden' }}>
        <div style={{
          width: `${w}%`, height: 6, borderRadius: 6,
          background: color,
          transition: 'width 1.2s cubic-bezier(.4,0,.2,1)',
        }} />
      </div>
    </div>
  );
}