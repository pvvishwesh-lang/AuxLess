import React from 'react';

const avatarColor = n =>
  ['#1DB954','#7C3AED','#EC4899','#F59E0B','#3B82F6'][n.charCodeAt(0) % 5];

export default function Avatar({ name, size = 34, active }) {
  const c = avatarColor(name);
  return (
    <div style={{
      width: size, height: size, borderRadius: '50%', flexShrink: 0,
      background: `${c}18`,
      border: `2px solid ${active ? c : 'transparent'}`,
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      fontSize: size * 0.34, fontWeight: 700, color: c,
      transition: 'border-color .3s',
    }}>
      {name}
    </div>
  );
}