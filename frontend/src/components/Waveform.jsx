import React from 'react';
import { T } from '../styles/tokens';

const HEIGHTS = [4,8,12,8,5,10,7,9,4,8,12,5,9,7];

export default function Waveform({ playing, bars = 14 }) {
  return (
    <div style={{ display:'flex', alignItems:'flex-end', gap:2, height:24 }}>
      {HEIGHTS.slice(0, bars).map((h, i) => (
        <div key={i} style={{
          width: 3, height: playing ? h * 2 : h,
          borderRadius: 2,
          background: playing ? T.green : T.dim,
          transformOrigin: 'bottom',
          animation: playing
            ? `wb ${.5 + i * .06}s ease-in-out ${i * .04}s infinite`
            : 'none',
          transition: 'height .3s, background .3s',
        }} />
      ))}
    </div>
  );
}