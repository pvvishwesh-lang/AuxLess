import React, { useState, useEffect } from 'react';
import { useAuth }       from './hooks/useAuth';
import { saveUserPrefs, exchangeCodeForToken } from './services/auth';
import Nav               from './components/Nav';
import Onboarding        from './pages/Onboarding';
import TabHome           from './pages/TabHome';
import TabCreate         from './pages/TabCreate';
import TabJoin           from './pages/TabJoin';
import TabAnalytics      from './pages/TabAnalytics';
import RoomView          from './pages/RoomView';
import AuthPage          from './pages/AuthPage';
import ProfilePage       from './pages/ProfilePage';
import SettingsPage      from './pages/SettingsPage';
import PlaylistsPage     from './pages/PlaylistsPage';
import { db, mlDb }      from './config/firebase';
import { doc, setDoc }   from 'firebase/firestore';

export default function App() {
  const { user, setUser, loading, logout } = useAuth();
  const [tab,         setTab]         = useState('home');

  const savedRoom = sessionStorage.getItem('auxless_room');
  const [screen,  setScreen]  = useState(savedRoom ? 'room' : 'main');
  const [roomId,  setRoomId]  = useState(savedRoom || null);
  const [showOnboard, setShowOnboard] = useState(false);

  // ── Handle YouTube OAuth callback ─────────────────────────
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const code   = params.get('code');
    if (!code || !user?.uid) return;

    // Clean URL immediately
    window.history.replaceState({}, '', window.location.pathname);

    exchangeCodeForToken(code).then(async (refreshToken) => {
      if (!refreshToken) {
        console.warn('[App] No refresh token returned');
        return;
      }
      console.log('[App] ✅ YouTube token exchanged for user:', user.uid);

      localStorage.setItem('auxless_refresh_token', refreshToken);

      // Save to YOUR Firestore
      await setDoc(doc(db, 'users', user.uid),
        { refreshToken }, { merge: true }
      ).catch(() => {});

      // Save to ML Firestore — pipeline reads from here!
      await setDoc(doc(mlDb, 'user_tokens', user.uid), {
        user_id:       user.uid,
        refresh_token: refreshToken,
        updatedAt:     Date.now(),
      }, { merge: true }).catch(() => {});

      // Update user state with new token
      setUser(u => ({ ...u, refreshToken }));

      // Check if user was trying to join a room before OAuth
      const pendingCode = sessionStorage.getItem('pending_join_code');
      if (pendingCode) {
        sessionStorage.removeItem('pending_join_code');
        setTab('join');
      }
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [user?.uid]);

  useEffect(() => {
    if (user?.uid && !user?.onboarded) {
      setShowOnboard(true);
    }
  }, [user?.uid, user?.onboarded]);

  const handleLogin = (userData) => {
    setUser(userData);
    setScreen('main');
    if (!userData?.onboarded) {
      setShowOnboard(true);
    }
  };

  const handleLogout = () => {
    logout();
    setUser(null);
    setTab('home');
    setScreen('main');
    sessionStorage.removeItem('auxless_room');
  };

  const handleOnboardDone = async (genres, artists) => {
    if (user?.uid) {
      await saveUserPrefs(user.uid, genres, artists);
      setUser(u => ({ ...u, onboarded: true, genres, artists }));
    }
    setShowOnboard(false);
    setTab('create');
  };

  const handleTabChange = (t) => {
    if (!user && (t === 'create' || t === 'join')) {
      setScreen('auth');
    } else {
      setTab(t);
    }
  };

  const handleEnterRoom = (code) => {
    setRoomId(code || 'AUX-7749');
    setScreen('room');
  };

  if (loading) {
    return (
      <div style={{ minHeight:'100vh', background:'#08080F', display:'flex', alignItems:'center', justifyContent:'center' }}>
        <div style={{ textAlign:'center' }}>
          <div style={{ width:48, height:48, borderRadius:14, margin:'0 auto 16px', background:'linear-gradient(135deg,#1DB954,#7C3AED)', display:'flex', alignItems:'center', justifyContent:'center', fontSize:22 }}>♫</div>
          <div style={{ width:28, height:28, border:'3px solid rgba(255,255,255,0.1)', borderTopColor:'#1DB954', borderRadius:'50%', animation:'spin .7s linear infinite', margin:'0 auto' }} />
        </div>
      </div>
    );
  }

  if (screen === 'room') {
    return (
      <RoomView
        roomId={roomId}
        user={user}
        onLeave={() => {
          sessionStorage.removeItem('auxless_room');
          setScreen('main');
          setTab('home');
        }}
      />
    );
  }

  if (screen === 'auth') {
    return (
      <AuthPage
        onLogin={(userData) => {
          handleLogin(userData);
          setTab('create');
        }}
        onSkip={() => {
          setScreen('main');
          setTab('join');
        }}
      />
    );
  }

  return (
    <div style={{ minHeight:'100vh', background:'#08080F', paddingBottom:64 }}>
      {showOnboard && <Onboarding onDone={handleOnboardDone} />}
      <Nav
        user={user}
        activeTab={tab}
        onTabChange={handleTabChange}
        onLogin={() => setScreen('auth')}
        onLogout={handleLogout}
      />
      <div style={{ position:'relative', zIndex:1 }}>
        {tab === 'home'      && <TabHome      onLogin={() => setScreen('auth')} onTab={setTab} />}
        {tab === 'create'    && <TabCreate    user={user} onEnterRoom={handleEnterRoom} />}
        {tab === 'join'      && <TabJoin      user={user} onEnterRoom={handleEnterRoom} />}
        {tab === 'analytics' && <TabAnalytics roomId={roomId} />}
        {tab === 'profile'   && <ProfilePage   user={user} />}
        {tab === 'settings'  && <SettingsPage  user={user} onLogout={handleLogout} />}
        {tab === 'playlists' && <PlaylistsPage user={user} />}
      </div>
    </div>
  );
}