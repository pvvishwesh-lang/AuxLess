import { useState, useEffect } from 'react';
import { listenAuth, logoutUser } from '../services/auth';
import { doc, getDoc }           from 'firebase/firestore';
import { db }                    from '../config/firebase';

export function useAuth() {
  const [user,    setUser]    = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const unsub = listenAuth(async (firebaseUser) => {
      if (firebaseUser) {
        // refresh JWT
        const token = await firebaseUser.getIdToken();
        localStorage.setItem('auxless_jwt', token);

        // get prefs + onboarded status from Firestore
        const snap = await getDoc(doc(db, 'users', firebaseUser.uid));
        const data = snap.exists() ? snap.data() : {};

        setUser({
          uid:       firebaseUser.uid,
          name:      firebaseUser.displayName,
          email:     firebaseUser.email,
          photoURL:  firebaseUser.photoURL,
          av:        firebaseUser.displayName
                       ?.split(' ')
                       .map(n => n[0])
                       .join('')
                       .slice(0, 2)
                       .toUpperCase() || 'U',
          // ── KEY: read from Firestore ──
          // false → show onboarding popup
          // true  → skip popup (returning user)
          onboarded: data.onboarded ?? false,
          genres:    data.genres    || [],
          artists:   data.artists   || [],
          token,
        });
      } else {
        setUser(null);
        localStorage.removeItem('auxless_jwt');
      }
      setLoading(false);
    });
    return () => unsub();
  }, []);

  const logout = async () => {
    await logoutUser();
    setUser(null);
  };

  return { user, setUser, loading, logout };
}