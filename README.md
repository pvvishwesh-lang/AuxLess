# AuxLess
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-000000?style=for-the-badge&logo=flask&logoColor=white)
![React](https://img.shields.io/badge/React-20232A?style=for-the-badge&logo=react&logoColor=61DAFB)
![Apache Beam](https://img.shields.io/badge/Apache%20Beam-E25A1C?style=for-the-badge&logo=apache&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google%20Cloud-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)
![Firestore](https://img.shields.io/badge/Firestore-FFCA28?style=for-the-badge&logo=firebase&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Scikit-Learn](https://img.shields.io/badge/Scikit--Learn-F7931E?style=for-the-badge&logo=scikitlearn&logoColor=white)
![PyTorch](https://img.shields.io/badge/PyTorch-EE4C2C?style=for-the-badge&logo=pytorch&logoColor=white)
![Pytest](https://img.shields.io/badge/Pytest-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white)
![Slack](https://img.shields.io/badge/Slack-4A154B?style=for-the-badge&logo=slack&logoColor=white)

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

### Backend
- Python 3.11
- Flask (Cloud Run REST API)

### Data Pipeline
- Apache Beam (Dataflow Runner)
- Google Cloud Dataflow
- Google Cloud Storage
- Google Cloud Firestore
- Google Cloud Pub/Sub
- Google Cloud Run
- Google Cloud Functions

### Data Processing & Validation
- Pandas
- NumPy
- SciPy
- Apache Beam transforms

### ML & Bias
- Scikit-Learn
- PyTorch
- TensorFlow Data Validation (TFDV)

### APIs
- YouTube Data API v3
- iTunes Search API
- OAuth 2.0 (Google)

### Frontend
- React
- Firebase (Firestore real-time listeners)

### Auth
- OAuth 2.0

### Testing
- pytest
- pytest-cov
- unittest.mock

### CI/CD
- GitHub Actions
- Google Cloud Build
- Docker
- Artifact Registry

### Monitoring & Alerting
- Cloud Logging
- Slack Webhooks

### Data Versioning
- GCS path-based versioning by session_id

---

## Backend Pipeline
The data pipeline is built on GCP and handles:
- Playlist ingestion via YouTube OAuth + iTunes metadata enrichment
- Distributed processing using Apache Beam on Dataflow
- Bias detection and mitigation across genre and country slices
- Real-time feedback processing via Pub/Sub streaming pipeline
- Schema validation, anomaly detection, and Slack alerting

See `backend/` for full pipeline documentation.

---

## Machine Learning
- Group based Genre clustering
- Feedback weighted ranking
- Online adaptation during sessions

---

## App Flow

![The flow of the app](https://github.com/pvvishwesh-lang/passtheaux/blob/main/docs/MLOps_HLD.png)

---

## Setup
### Prerequisites
- Python 3.11+
- GCP account with Cloud Run, Dataflow, Firestore, Pub/Sub, GCS enabled
- YouTube OAuth credentials

---

### Installation
```bash
git clone <repo_url>
cd <repo_name>
pip install -r requirements.txt
```
---

### Running Tests
```bash
pytest tests/api_tests/ -v --cov=backend
```

---

## Privacy
- User consented data only
- Session based processing
- No long term storage of playlist

---

## Status
In Development
