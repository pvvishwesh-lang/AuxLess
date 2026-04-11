import { initializeApp }               from 'firebase/app';
import { getAuth, GoogleAuthProvider } from 'firebase/auth';
import { getFirestore }                from 'firebase/firestore';

// ── YOUR project — Google Auth + users + rooms ────────────────
const firebaseConfig = {
  apiKey:            process.env.REACT_APP_FIREBASE_API_KEY,
  authDomain:        process.env.REACT_APP_FIREBASE_AUTH_DOMAIN,
  projectId:         process.env.REACT_APP_FIREBASE_PROJECT_ID,
  storageBucket:     process.env.REACT_APP_FIREBASE_STORAGE_BUCKET,
  messagingSenderId: process.env.REACT_APP_FIREBASE_MESSAGING_SENDER_ID,
  appId:             process.env.REACT_APP_FIREBASE_APP_ID,
};

// ── NIKHIL'S project — sessions + pipeline + ML ───────────────
const mlFirebaseConfig = {
  apiKey:            process.env.REACT_APP_ML_FIREBASE_API_KEY,
  authDomain:        process.env.REACT_APP_ML_FIREBASE_AUTH_DOMAIN,
  projectId:         process.env.REACT_APP_ML_FIREBASE_PROJECT_ID,
  storageBucket:     process.env.REACT_APP_ML_FIREBASE_STORAGE_BUCKET,
  messagingSenderId: process.env.REACT_APP_ML_FIREBASE_MESSAGING_SENDER_ID,
  appId:             process.env.REACT_APP_ML_FIREBASE_APP_ID,
};

const app   = initializeApp(firebaseConfig);
const mlApp = initializeApp(mlFirebaseConfig, 'ml');

// YOUR Firestore — users, rooms
export const db       = getFirestore(app);

// NIKHIL'S Firestore — sessions, song_events, recommendations, tracks
export const mlDb     = getFirestore(mlApp, 'auxless');

export const auth     = getAuth(app);
export const provider = new GoogleAuthProvider();