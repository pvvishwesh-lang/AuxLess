import {
  signInWithPopup,
  signOut,
  onAuthStateChanged,
  GoogleAuthProvider,
} from 'firebase/auth';
import { doc, setDoc, getDoc } from 'firebase/firestore';
import { auth, db, mlDb, provider } from '../config/firebase';

// Add YouTube scope
provider.addScope('https://www.googleapis.com/auth/youtube.readonly');
provider.setCustomParameters({ 
  access_type: 'offline',
  prompt: 'consent',
});

export const signInWithGoogle = async () => {
  const result = await signInWithPopup(auth, provider);
  const user   = result.user;

  const credential   = GoogleAuthProvider.credentialFromResult(result);
  const refreshToken = result._tokenResponse?.oauthRefreshToken || '';

  console.log('[Auth] Refresh token captured:', refreshToken ? '✅' : '❌');

  const token = await user.getIdToken();
  localStorage.setItem('auxless_jwt', token);

  // Do not store OAuth refresh tokens in client-side localStorage.

  const userRef = doc(db, 'users', user.uid);
  const snap    = await getDoc(userRef);

  if (!snap.exists()) {
    await setDoc(userRef, {
      uid:          user.uid,
      name:         user.displayName,
      email:        user.email,
      photoURL:     user.photoURL,
      genres:       [],
      artists:      [],
      createdAt:    Date.now(),
      onboarded:    false,
      refreshToken: refreshToken,
    });
  } else if (refreshToken) {
    await setDoc(userRef, { refreshToken }, { merge: true });
  }

  // Also save to ML Firestore immediately on sign in
  if (refreshToken) {
    try {
      await setDoc(
        doc(mlDb, 'user_tokens', user.uid),
        {
          user_id:       user.uid,
          refresh_token: refreshToken,
          updatedAt:     Date.now(),
        },
        { merge: true }
      );
      console.log('[Auth] ✅ YouTube token saved to ML Firestore');
    } catch (e) {
      console.warn('[Auth] ML token save failed:', e.message);
    }
  }

  return {
    uid:          user.uid,
    name:         user.displayName,
    email:        user.email,
    photoURL:     user.photoURL,
    av:           user.displayName
                    ?.split(' ')
                    .map(n => n[0])
                    .join('')
                    .slice(0, 2)
                    .toUpperCase() || 'U',
    onboarded:    snap.exists() ? (snap.data().onboarded ?? false) : false,
    genres:       snap.exists() ? (snap.data().genres   || []) : [],
    artists:      snap.exists() ? (snap.data().artists  || []) : [],
    refreshToken: refreshToken,
    token,
  };
};

export const saveUserPrefs = async (uid, genres, artists) => {
  const refreshToken = localStorage.getItem('auxless_refresh_token') || '';

  // 1. Save to YOUR Firestore
  await setDoc(
    doc(db, 'users', uid),
    { genres, artists, onboarded: true, refreshToken },
    { merge: true }
  );

  // 2. Save to ML Firestore — pipeline reads from here!
  try {
    await setDoc(
      doc(mlDb, 'user_tokens', uid),
      {
        user_id:       uid,
        refresh_token: refreshToken,
        genres,
        artists,
        updatedAt:     Date.now(),
      },
      { merge: true }
    );
    console.log('[Auth] ✅ YouTube token saved to ML Firestore');
  } catch (e) {
    console.warn('[Auth] ML token save failed:', e.message);
  }
};

export const logoutUser = async () => {
  await signOut(auth);
  localStorage.removeItem('auxless_jwt');
  localStorage.removeItem('auxless_refresh_token');
};
export const exchangeCodeForToken = async (code) => {
  try {
    const response = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        code,
        client_id:     process.env.REACT_APP_YOUTUBE_CLIENT_ID,
        client_secret: process.env.REACT_APP_YOUTUBE_CLIENT_SECRET,
        redirect_uri:  window.location.origin,
        grant_type:    'authorization_code',
      }),
    });
    const data = await response.json();
    console.log('[Auth] Token exchange response:', data);
    return data.refresh_token || '';
  } catch (e) {
    console.warn('[Auth] Token exchange failed:', e);
    return '';
  }
};

export const getYouTubeAuthUrl = () => {
  const clientId = process.env.REACT_APP_YOUTUBE_CLIENT_ID;
  const redirectUri = encodeURIComponent(window.location.origin);
  const scope       = encodeURIComponent('https://www.googleapis.com/auth/youtube.readonly');
  return `https://accounts.google.com/o/oauth2/auth?client_id=${clientId}&redirect_uri=${redirectUri}&response_type=code&scope=${scope}&access_type=offline&prompt=consent`;
};

export const listenAuth = (callback) =>
  onAuthStateChanged(auth, callback);