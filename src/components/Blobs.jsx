import React from 'react';
import { T } from '../styles/tokens';

const BLOBS = [
  { l:'8%',  t:'20%', w:600, c:T.purple, a:'b0', d:10 },
  { l:'82%', t:'55%', w:500, c:T.green,  a:'b1', d:13 },
  { l:'48%', t:'88%', w:420, c:T.pink,   a:'b2', d:9  },
];

export default function Blobs() {
  return (
    <div style={{ position:'fixed', inset:0, overflow:'hidden', pointerEvents:'none', zIndex:0 }}>
      {BLOBS.map((b, i) => (
        <div key={i} style={{
          position: 'absolute', left: b.l, top: b.t,
          width: b.w, height: b.w, borderRadius: '50%',
          background: b.c, opacity: 0.065,
          filter: 'blur(100px)',
          transform: 'translate(-50%,-50%)',
          animation: `${b.a} ${b.d}s ease-in-out infinite alternate`,
        }} />
      ))}
    </div>
  );
}