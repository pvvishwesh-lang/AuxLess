import React from 'react';
import { T } from '../styles/tokens';

export default function Pill({ children, on, onClick, color = T.green }) {
  return (
    <button
      onClick={onClick}
      style={{
        padding: '7px 15px', borderRadius: 999,
        fontSize: 13, fontWeight: 500, cursor: 'pointer',
        border: `1.5px solid ${on ? color : 'rgba(255,255,255,0.09)'}`,
        background: on ? `${color}18` : 'transparent',
        color: on ? color : T.muted,
        transition: 'all .18s',
      }}
    >
      {children}
    </button>
  );
}