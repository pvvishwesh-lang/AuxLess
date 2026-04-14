# AuxLess Frontend

A production-ready React web app for the AuxLess party music recommendation system. The frontend connects users, rooms, and all three backend pipelines (batch, streaming, ML) through Firebase Firestore as the central data layer.

---

## Tech Stack

- **React 19** — component-based UI
- **Firebase** — Google Auth + Firestore real-time listeners
- **Google Cloud Firestore** — central data layer for all pipelines
- **Tailwind CSS + inline styles** — responsive design
- **YouTube Data API** — playlist fetching via OAuth refresh token

---

## Architecture

```
User logs in (Google OAuth)
        ↓
Onboarding popup — genre + artist preferences saved to Firestore
        ↓
Host creates room → SHA256 session ID generated
        ↓
Session doc written to Firestore (status: pending)
        ↓
Batch pipeline auto-triggers via Cloud Function + Pub/Sub
        ↓
YouTube playlists fetched using OAuth refresh token
        ↓
ML writes recommendations → Firestore onSnapshot → queue updates live
        ↓
Users vote (like/dislike) → feedback_events → streaming pipeline
        ↓
Scores update in real time across all devices in the room
```

---

## Why Two Firebase Projects?

The frontend uses **two separate Firebase connections**:

| Project | Database | Used for |
|---|---|---|
| `auxless-30b3e` | default | Google Auth, users, rooms |
| `flash-aviary-491923-h1` | auxless | Sessions, pipeline data, ML recommendations |

**Reason:** The batch pipeline, streaming pipeline, and ML model all run in Nikhil's GCP project (`flash-aviary-491923-h1`) and write directly to that Firestore. The frontend needs to read/write there too so all parts of the system share the same data. Authentication and room management stay in our own Firebase project (`auxless-30b3e`) to keep concerns separated.

Both connections are initialized in `src/config/firebase.js`:
- `db` → your project (auth, users, rooms)
- `mlDb` → Nikhil's project (sessions, recommendations, pipeline events)

---

## Prerequisites

Install these before starting:

### 1. Node.js + npm
Download and install from [nodejs.org](https://nodejs.org/) (v18 or higher recommended).

Verify installation:
```bash
node -v    # should print v18.x.x or higher
npm -v     # should print 9.x.x or higher
```

### 2. Git
Download from [git-scm.com](https://git-scm.com/) if not already installed.

```bash
git --version    # should print git version 2.x.x
```

---

## Setup Instructions

### Step 1 — Clone the repo
```bash
git clone https://github.com/pvvishwesh-lang/AuxLess.git
cd AuxLess/frontend
```

### Step 2 — Install all dependencies
```bash
npm install
```
This installs React, Firebase, Tailwind, and all other libraries from `package.json`. Takes 1-2 minutes.

### Step 3 — Set up environment variables
Create a `.env` file inside the `frontend/` folder:
```bash
cp .env.example .env
```

Open `.env` and fill in the values (ask Vaishnavi for these):
```env
# ── YOUR Firebase (auth + rooms) ──────────────────────────────
REACT_APP_FIREBASE_API_KEY=
REACT_APP_FIREBASE_AUTH_DOMAIN=
REACT_APP_FIREBASE_PROJECT_ID=
REACT_APP_FIREBASE_STORAGE_BUCKET=
REACT_APP_FIREBASE_MESSAGING_SENDER_ID=
REACT_APP_FIREBASE_APP_ID=

# ── NIKHIL'S Firebase (sessions + pipeline + ML) ──────────────
REACT_APP_ML_FIREBASE_API_KEY=
REACT_APP_ML_FIREBASE_AUTH_DOMAIN=
REACT_APP_ML_FIREBASE_PROJECT_ID=
REACT_APP_ML_FIREBASE_STORAGE_BUCKET=
REACT_APP_ML_FIREBASE_MESSAGING_SENDER_ID=
REACT_APP_ML_FIREBASE_APP_ID=

# ── YouTube OAuth refresh token ────────────────────────────────
REACT_APP_YOUTUBE_REFRESH_TOKEN=

# ── Streaming Pipeline Cloud Function ─────────────────────────
REACT_APP_CLOUD_FUNCTION_URL=
```

> ⚠️ Never commit `.env` to git — it contains secret keys.

### Step 4 — Run the app
```bash
npm start
```
App opens automatically at **http://localhost:3000**

---

## Getting YouTube Refresh Token

The pipeline needs a YouTube OAuth refresh token to fetch playlists. Each team member needs their own.

1. Get the `client_secret_*.json` file from Vishwesh
2. Save it to your machine
3. Install the required library:
```bash
pip install google-auth-oauthlib
```
4. Run the tester script (from backend folder):
```bash
python tester.py
```
5. Browser opens → sign in with your Google account
6. Copy the **Refresh token** printed in terminal
7. Paste it in `.env` as `REACT_APP_YOUTUBE_REFRESH_TOKEN`

> Note: Your Google account must be added as a test user by Vishwesh before running this.

---

## Firestore Structure

```
auxless-30b3e (YOUR Firebase — db)
├── users/{uid}
│   ├── name, email, photoURL
│   ├── genres, artists           ← from onboarding
│   └── onboarded: true/false
└── rooms/{code}
    ├── host_uid, host_name
    ├── status: active
    └── users: [{uid, name, isHost, isactive}]

flash-aviary-491923-h1/auxless (NIKHIL'S Firebase — mlDb)
└── sessions/{sessionId}
    ├── session_id, room_code
    ├── status: pending → running → done
    ├── users: [{user_id, refresh_token, genres, artists}]
    ├── metadata/state
    │   ├── songs_played_count
    │   └── session_number
    ├── song_events/{auto}
    │   ├── video_id, play_order
    │   └── liked_flag: 0 or 1
    ├── feedback_events/{auto}
    │   └── event_type: play/like/dislike/complete
    ├── tracks/{video_id}
    │   └── score             ← written by streaming pipeline
    ├── user_feedback/{uid_videoId}
    │   └── action, score_delta
    └── recommendations/{rank}
        └── track_title, genre, final_score, video_id   ← written by ML
```

---

## File Structure

```
frontend/
├── public/
└── src/
    ├── config/
    │   └── firebase.js              # Two Firebase app connections (db + mlDb)
    ├── services/
    │   ├── auth.js                  # Google OAuth + YouTube scope + Firestore user save
    │   ├── pipelineService.js       # All Firestore writes for pipeline + ML
    │   └── artistImageService.js    # Artist image fetching
    ├── hooks/
    │   └── useAuth.js               # Firebase auth state listener
    ├── pages/
    │   ├── RoomView.jsx             # Main room — queue, voting, ML recommendations, YouTube player
    │   ├── TabCreate.jsx            # Room creation + session init + pipeline trigger
    │   ├── TabJoin.jsx              # Join room + add user to session
    │   ├── AuthPage.jsx             # Google sign-in page
    │   ├── Onboarding.jsx           # Genre + artist preference collection
    │   └── TabAnalytics.jsx         # Platform analytics
    └── components/
        ├── Nav.jsx                  # Sticky navbar + avatar dropdown
        ├── Avatar.jsx
        ├── Waveform.jsx
        ├── Blobs.jsx
        └── ...
```

---

## Key Features

- **Google OAuth 2.0** login with YouTube scope to capture refresh token
- **Room creation** with SHA256 session ID (required for Dataflow job naming)
- **Batch pipeline** auto-triggers when session `status: pending` is written
- **ML recommendations** appear in queue automatically via `onSnapshot`
- **Streaming pipeline** receives every play, like, dislike, skip event
- **YouTube player** embedded for real audio playback
- **Live voting** with scores persisting across queue updates
- **30-second song timer** with session tracker
- **Page refresh** restores room session automatically

---

## Troubleshooting

**`npm install` fails**
- Make sure Node.js v18+ is installed: `node -v`
- Try: `npm install --legacy-peer-deps`

**App won't start**
- Make sure `.env` file exists in `frontend/` folder
- All `REACT_APP_*` variables must be filled in

**Firebase permission errors in console**
- Firestore rules in `flash-aviary-491923-h1` must allow read/write
- Ask Nikhil to check Firebase Console → Firestore → Rules

**Pipeline not triggering (status stays pending)**
- Check `REACT_APP_YOUTUBE_REFRESH_TOKEN` is filled in `.env`
- Token must be from an account added as test user by Vishwesh

**Songs showing "Unknown" in queue**
- Create a new room and wait 5-10 mins for pipeline + ML to finish
- Check Firestore → sessions → your session → recommendations subcollection
