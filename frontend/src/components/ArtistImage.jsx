import React, { useState, useEffect } from 'react';
import { getArtistImage } from '../services/artistImageService';

const getColor = (name = '') => {
  const colors = ['#1DB954','#7C3AED','#EC4899','#F59E0B','#3B82F6','#10B981','#EF4444','#8B5CF6'];
  let hash = 0;
  for (let i = 0; i < name.length; i++) hash = name.charCodeAt(i) + ((hash << 5) - hash);
  return colors[Math.abs(hash) % colors.length];
};

export default function ArtistImage({ name, size = 40, radius = 10, style = {} }) {
  const [src, setSrc]     = useState(null);
  const [loaded, setLoaded] = useState(false);
  const color             = getColor(name);
  const initials          = (name || '?').split(' ').map(n => n[0]).join('').slice(0, 2).toUpperCase();

  useEffect(() => {
    if (!name) return;
    getArtistImage(name).then(url => {
      if (url) setSrc(url);
    });
  }, [name]);

  return (
    <div style={{
      width: size, height: size, borderRadius: radius,
      overflow: 'hidden', flexShrink: 0,
      background: `${color}22`,
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      fontSize: size * 0.32, fontWeight: 700, color,
      position: 'relative',
      ...style,
    }}>
      {src && (
        <img
          src={src}
          alt={name}
          onLoad={() => setLoaded(true)}
          onError={() => setSrc(null)}
          style={{
            position: 'absolute', inset: 0,
            width: '100%', height: '100%',
            objectFit: 'cover',
            opacity: loaded ? 1 : 0,
            transition: 'opacity 0.3s',
          }}
        />
      )}
      {/* initials shown until image loads */}
      <span style={{ opacity: loaded ? 0 : 1, transition: 'opacity 0.3s', position: 'absolute' }}>
        {initials}
      </span>
    </div>
  );
}