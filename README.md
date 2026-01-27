# AuxLess

## Party Music Recommendation System

A real-time, multi user music recommendation system designed for social settings. The system aggregates playlist data from every single user, clusters the songs by genres and adapts playback using live user feedback.

---

## Features
- Multi User party rooms
- Genre based clustering
- Real time like and dislike based feedback loop
- Adaptive group recommendations

---

## System Overview
- Users join a common room and authenticate via a music provider
- Playlist and track metadata is fetched
- Tracks are clustered based on genre
- Songs from dominant clusters are prioritized
- Feedback influences subsequent recommendations

---

## Tech Stack
- Backend: Python
- ML: Scikit-Learn/PyTorck
- API: Spotify Web API
- Frontend: React
- Auth: OAuth 2.0

---

## Machine Learning
- Group based Genre clustering
- Feedback weighted ranking
- Online adaptation during sessions

---

## App Flow

![The flow of the app](https://github.com/pvvishwesh-lang/passtheaux/blob/main/docs/MLOps_HLD.png)

---

## Privacy
- User consented data only
- Session based processing
- No long term storage of playlist

---

## Status
In Development
