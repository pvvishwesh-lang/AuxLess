import {
  signInWithPopup,
  signOut,
  onAuthStateChanged,
} from 'firebase/auth';
import { doc, setDoc, getDoc } from 'firebase/firestore';
import { auth, db, provider, mlDb } from '../config/firebase';

// ── Add YouTube scope to get refresh token ────────────────────
provider.addScope('https://www.googleapis.com/auth/youtube.readonly');
provider.setCustomParameters({ access_type: 'offline' });

// ── Google sign-in popup ──────────────────────────────────────
export const signInWithGoogle = async () => {
  const result = await signInWithPopup(auth, provider);
  const user   = result.user;

  // ── Get YouTube refresh token from OAuth result ───────────
  const credential     = result._tokenResponse;
  const refreshToken   = credential?.refreshToken || result._tokenResponse?.oauthRefreshToken || '';

  // get Firebase JWT id token
  const token = await user.getIdToken();
  localStorage.setItem('auxless_jwt', token);

  // save refresh token locally too
  if (refreshToken) {
    localStorage.setItem('auxless_refresh_token', refreshToken);
  }

  // check if user already exists in YOUR Firestore
  const userRef = doc(db, 'users', user.uid);
  const snap    = await getDoc(userRef);

  if (!snap.exists()) {
    // NEW USER — create doc with onboarded: false
    await setDoc(userRef, {
      uid:          user.uid,
      name:         user.displayName,
      email:        user.email,
      photoURL:     user.photoURL,
      genres:       [],
      artists:      [],
      createdAt:    Date.now(),
      onboarded:    false,
      refreshToken: refreshToken, // save for reference
    });
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

// ── Save onboarding prefs + refresh token to both Firestores ──
export const saveUserPrefs = async (uid, genres, artists) => {
  const refreshToken = localStorage.getItem('auxless_refresh_token') || '';

  // 1. Save to YOUR Firestore — users collection
  await setDoc(
    doc(db, 'users', uid),
    { genres, artists, onboarded: true, refreshToken },
    { merge: true }
  );

  // 2. Save refresh token to NIKHIL'S Firestore — so pipeline can use it!
  // Pipeline reads refresh_token from sessions/{id}/users array
  // We store it in a users collection for easy lookup
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
    console.log('[Auth] ✅ Refresh token saved to ML Firestore');
  } catch (e) {
    console.warn('[Auth] ML Firestore token save failed:', e.message);
  }
};

// ── Sign out ──────────────────────────────────────────────────
export const logoutUser = async () => {
  await signOut(auth);
  localStorage.removeItem('auxless_jwt');
  localStorage.removeItem('auxless_refresh_token');
};

// ── Auth state listener ───────────────────────────────────────
export const listenAuth = (callback) =>
  onAuthStateChanged(auth, callback);