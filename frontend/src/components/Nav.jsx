import React, { useState, useRef, useEffect } from 'react';
import { T } from '../styles/tokens';
import Avatar from './Avatar';
import Btn from './Btn';

const TABS = [
  { id:'home',      icon:'✦',  label:'Home'      },
  { id:'create',    icon:'🎛️', label:'Create'    },
  { id:'join',      icon:'🔑', label:'Join'      },
  { id:'analytics', icon:'📊', label:'Analytics' },
];

export default function Nav({ user, activeTab, onTabChange, onLogin, onLogout }) {
  const [sidebarOpen,  setSidebarOpen]  = useState(false);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const dropRef = useRef(null);

  // close dropdown when clicking outside
  useEffect(() => {
    const handler = (e) => {
      if (dropRef.current && !dropRef.current.contains(e.target)) {
        setDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const handleTab = (id) => {
    onTabChange(id);
    setSidebarOpen(false);
  };

  return (
    <>
      {/* ── TOP NAV ── */}
      <nav style={{
        position: 'sticky', top: 0, zIndex: 50,
        backdropFilter: 'blur(18px)',
        background: 'rgba(8,8,15,.92)',
        borderBottom: `1px solid ${T.border}`,
      }}>
        <div style={{
          maxWidth: 900, margin: '0 auto',
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          padding: '0 20px', height: 60,
        }}>

          {/* LOGO */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 9, flexShrink: 0 }}>
            <div style={{
              width: 30, height: 30, borderRadius: 8,
              background: `linear-gradient(135deg,${T.green},${T.purple})`,
              display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 15,
            }}>♫</div>
            <span style={{ fontWeight: 900, fontSize: 16, letterSpacing: '-0.5px', color: T.text }}>
              AuxLess
            </span>
          </div>

          {/* DESKTOP TABS */}
          <div style={{
            display: 'flex', gap: 2,
            background: 'rgba(255,255,255,0.04)',
            borderRadius: 12, padding: 4,
          }}>
            {TABS.map(t => (
              <button key={t.id} onClick={() => handleTab(t.id)} style={{
                display: 'flex', alignItems: 'center', gap: 6,
                padding: '7px 16px', borderRadius: 9,
                fontSize: 13, fontWeight: 600, cursor: 'pointer', border: 'none',
                background: activeTab === t.id ? T.card : 'transparent',
                color: activeTab === t.id ? T.text : T.muted,
                transition: 'all .2s',
              }}>
                <span style={{ fontSize: 14 }}>{t.icon}</span>
                <span>{t.label}</span>
                {activeTab === t.id && (
                  <span style={{ width: 4, height: 4, borderRadius: '50%', background: T.green }} />
                )}
              </button>
            ))}
          </div>

          {/* RIGHT — avatar dropdown + hamburger */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexShrink: 0 }}>

            {user ? (
              /* ── AVATAR DROPDOWN ── */
              <div ref={dropRef} style={{ position: 'relative' }}>
                <button
                  onClick={() => setDropdownOpen(o => !o)}
                  style={{
                    display: 'flex', alignItems: 'center', gap: 8,
                    padding: '5px 12px 5px 5px',
                    background: dropdownOpen ? 'rgba(255,255,255,0.08)' : 'rgba(255,255,255,0.04)',
                    border: `1px solid ${dropdownOpen ? T.borderH : T.border}`,
                    borderRadius: 999, cursor: 'pointer', transition: 'all .2s',
                  }}
                >
                  <Avatar name={user.av} size={28} active />
                  <span style={{ fontSize: 13, fontWeight: 600, color: T.text }}>
                    {user.name?.split(' ')[0]}
                  </span>
                  <span style={{
                    fontSize: 10, color: T.muted,
                    display: 'inline-block',
                    transform: dropdownOpen ? 'rotate(180deg)' : 'none',
                    transition: 'transform .2s',
                  }}>▾</span>
                </button>

                {/* DROPDOWN MENU */}
                {dropdownOpen && (
                  <div style={{
                    position: 'absolute', top: 'calc(100% + 8px)', right: 0,
                    width: 220, background: T.surface,
                    border: `1px solid ${T.border}`, borderRadius: 14,
                    padding: '8px', zIndex: 100,
                    boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
                    animation: 'fadeUp .18s ease',
                  }}>
                    {/* user info */}
                    <div style={{
                      padding: '10px 12px 12px',
                      borderBottom: `1px solid ${T.border}`, marginBottom: 6,
                    }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                        <Avatar name={user.av} size={36} active />
                        <div>
                          <div style={{ fontSize: 14, fontWeight: 700, color: T.text }}>
                            {user.name}
                          </div>
                          <div style={{ fontSize: 11, color: T.green, marginTop: 2 }}>
                            ● Active
                          </div>
                        </div>
                      </div>
                    </div>

                    {/* menu items */}
                   {[
  { icon: '👤', label: 'Profile',      id: 'profile'    },
  { icon: '⚙️', label: 'Settings',     id: 'settings'   },
  { icon: '🎵', label: 'My playlists', id: 'playlists'  },
].map(item => (
  <button key={item.label}
    onClick={() => { onTabChange(item.id); setDropdownOpen(false); }}
    style={{
      width: '100%', display: 'flex', alignItems: 'center', gap: 10,
      padding: '9px 12px', borderRadius: 8, border: 'none',
      background: 'transparent', color: T.muted,
      fontSize: 13, fontWeight: 500, cursor: 'pointer',
      transition: 'all .15s', textAlign: 'left',
    }}
    onMouseEnter={e => { e.currentTarget.style.background='rgba(255,255,255,0.06)'; e.currentTarget.style.color=T.text; }}
    onMouseLeave={e => { e.currentTarget.style.background='transparent'; e.currentTarget.style.color=T.muted; }}
  >
    <span>{item.icon}</span>{item.label}
  </button>
))}
                    {/* sign out */}
                    <div style={{ borderTop: `1px solid ${T.border}`, marginTop: 6, paddingTop: 6 }}>
                      <button
                        onClick={() => { onLogout(); setDropdownOpen(false); }}
                        style={{
                          width: '100%', display: 'flex', alignItems: 'center', gap: 10,
                          padding: '9px 12px', borderRadius: 8, border: 'none',
                          background: 'transparent', color: '#EF4444',
                          fontSize: 13, fontWeight: 600, cursor: 'pointer', textAlign: 'left',
                          transition: 'background .15s',
                        }}
                        onMouseEnter={e => e.currentTarget.style.background = 'rgba(239,68,68,0.1)'}
                        onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
                      >
                        🚪 Sign out
                      </button>
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <Btn onClick={onLogin} sm>Sign in</Btn>
            )}

            {/* HAMBURGER */}
            <button onClick={() => setSidebarOpen(o => !o)} style={{
              width: 36, height: 36, borderRadius: 9,
              background: sidebarOpen ? T.greenLo : 'rgba(255,255,255,0.05)',
              border: `1px solid ${sidebarOpen ? T.green : T.border}`,
              display: 'flex', flexDirection: 'column',
              alignItems: 'center', justifyContent: 'center', gap: 5,
              cursor: 'pointer', transition: 'all .2s',
            }}>
              {[0, 1, 2].map(i => (
                <span key={i} style={{
                  display: 'block', width: 16, height: 1.5, borderRadius: 2,
                  background: sidebarOpen ? T.green : T.muted,
                  transition: 'all .25s',
                  transform: sidebarOpen
                    ? i === 0 ? 'translateY(3.5px) rotate(45deg)'
                    : i === 2 ? 'translateY(-3.5px) rotate(-45deg)' : 'scaleX(0)'
                    : 'none',
                  opacity: sidebarOpen && i === 1 ? 0 : 1,
                }} />
              ))}
            </button>
          </div>
        </div>

        {/* MOBILE BOTTOM TAB BAR */}
        <div style={{ display: 'flex', borderTop: `1px solid ${T.border}` }}>
          {TABS.map(t => (
            <button key={t.id} onClick={() => handleTab(t.id)} style={{
              flex: 1, padding: '10px 4px 12px',
              display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 3,
              border: 'none',
              background: activeTab === t.id ? `${T.green}0F` : 'transparent',
              cursor: 'pointer',
              borderTop: `2px solid ${activeTab === t.id ? T.green : 'transparent'}`,
              transition: 'all .2s',
            }}>
              <span style={{ fontSize: 18 }}>{t.icon}</span>
              <span style={{ fontSize: 10, fontWeight: 600, color: activeTab === t.id ? T.green : T.muted }}>
                {t.label}
              </span>
            </button>
          ))}
        </div>
      </nav>

      {/* SIDEBAR OVERLAY */}
      {sidebarOpen && (
        <div onClick={() => setSidebarOpen(false)} style={{
          position: 'fixed', inset: 0, zIndex: 98,
          background: 'rgba(0,0,0,0.55)', backdropFilter: 'blur(4px)',
        }} />
      )}

      {/* SIDEBAR PANEL */}
      <div style={{
        position: 'fixed', top: 0, right: 0, bottom: 0, zIndex: 99,
        width: 280, background: T.surface,
        borderLeft: `1px solid ${T.border}`,
        transform: sidebarOpen ? 'translateX(0)' : 'translateX(100%)',
        transition: 'transform .3s cubic-bezier(.4,0,.2,1)',
        display: 'flex', flexDirection: 'column',
      }}>
        {/* sidebar header */}
        <div style={{
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          padding: '18px 20px', borderBottom: `1px solid ${T.border}`,
        }}>
          <span style={{ fontWeight: 800, fontSize: 15, color: T.text }}>Menu</span>
          <button onClick={() => setSidebarOpen(false)} style={{
            width: 28, height: 28, borderRadius: 7,
            border: `1px solid ${T.border}`, background: 'transparent',
            color: T.muted, cursor: 'pointer', fontSize: 14,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>✕</button>
        </div>

        {/* user card in sidebar */}
        {user && (
          <div style={{
            margin: '12px 16px', padding: '14px',
            background: T.card, borderRadius: 12, border: `1px solid ${T.border}`,
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <Avatar name={user.av} size={40} active />
              <div>
                <div style={{ fontSize: 14, fontWeight: 700, color: T.text }}>{user.name}</div>
                <div style={{ fontSize: 11, color: T.green, marginTop: 2 }}>● Online</div>
              </div>
            </div>
          </div>
        )}

        {/* sidebar nav links */}
        <div style={{ flex: 1, padding: '8px 12px', overflowY: 'auto' }}>
          {TABS.map(t => (
            <button key={t.id} onClick={() => handleTab(t.id)} style={{
              width: '100%', display: 'flex', alignItems: 'center', gap: 12,
              padding: '12px 14px', borderRadius: 10, marginBottom: 4,
              border: 'none', cursor: 'pointer', textAlign: 'left',
              background: activeTab === t.id ? T.greenLo : 'transparent',
              transition: 'all .18s',
            }}>
              <span style={{
                width: 36, height: 36, borderRadius: 9, flexShrink: 0,
                background: activeTab === t.id ? T.greenMid : 'rgba(255,255,255,0.05)',
                display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 16,
              }}>{t.icon}</span>
              <span style={{
                fontSize: 14, fontWeight: 600,
                color: activeTab === t.id ? T.green : T.muted,
              }}>{t.label}</span>
              {activeTab === t.id && (
                <span style={{ marginLeft: 'auto', width: 6, height: 6, borderRadius: '50%', background: T.green }} />
              )}
            </button>
          ))}
        </div>

        {/* sidebar footer */}
        <div style={{ padding: '16px 20px', borderTop: `1px solid ${T.border}` }}>
          {user
            ? <button onClick={() => { onLogout(); setSidebarOpen(false); }} style={{
                width: '100%', padding: '11px', borderRadius: 10,
                background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.2)',
                color: '#EF4444', fontSize: 13, fontWeight: 600, cursor: 'pointer',
              }}>🚪 Sign out</button>
            : <button onClick={() => { onLogin(); setSidebarOpen(false); }} style={{
                width: '100%', padding: '11px', borderRadius: 10,
                background: T.greenLo, border: `1px solid ${T.green}44`,
                color: T.green, fontSize: 13, fontWeight: 600, cursor: 'pointer',
              }}>Sign in with Google</button>
          }
          <p style={{ fontSize: 11, color: T.dim, textAlign: 'center', marginTop: 10 }}>
            AuxLess v0.1 · Party music ML
          </p>
        </div>
      </div>
    </>
  );
}