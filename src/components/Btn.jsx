import React from 'react';
import { T } from '../styles/tokens';

export default function Btn({
  children, onClick, variant = 'green',
  full, disabled, sm, icon, style = {}
}) {
  const bg  = variant === 'green'   ? T.green
             : variant === 'purple' ? T.purple
             : variant === 'ghost'  ? 'rgba(255,255,255,0.07)'
             : 'transparent';
  const col = (variant === 'outline' || variant === 'ghost') ? T.text : '#060610';
  const brd = variant === 'outline' ? '1.5px solid rgba(255,255,255,0.16)' : 'none';

  return (
    <button
      disabled={disabled}
      onClick={onClick}
      style={{
        width: full ? '100%' : 'auto',
        padding: sm ? '8px 18px' : '12px 26px',
        borderRadius: 12,
        fontSize: sm ? 13 : 14,
        fontWeight: 600,
        cursor: disabled ? 'not-allowed' : 'pointer',
        border: brd,
        background: disabled ? 'rgba(255,255,255,0.05)' : bg,
        color: disabled ? T.dim : col,
        transition: 'opacity .18s, transform .12s',
        opacity: disabled ? 0.45 : 1,
        display: 'flex', alignItems: 'center',
        justifyContent: 'center', gap: 8,
        ...style,
      }}
      onMouseEnter={e => !disabled && (e.currentTarget.style.opacity = '.82')}
      onMouseLeave={e => !disabled && (e.currentTarget.style.opacity = '1')}
      onMouseDown={e  => !disabled && (e.currentTarget.style.transform = 'scale(.97)')}
      onMouseUp={e    => !disabled && (e.currentTarget.style.transform = 'scale(1)')}
    >
      {icon && <span style={{ fontSize: 16 }}>{icon}</span>}
      {children}
    </button>
  );
}