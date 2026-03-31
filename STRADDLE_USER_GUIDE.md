# Straddle Data Engine - User & Operations Guide

This guide describes how to operate, run, scaled, and understand the internal components of the `straddle_Data_Make` subsystem.

## Overview
The `straddle_Data_Make` service is an autonomous ingestion engine paired directly alongside the primary `market_ingest_final` logic. It operates in two layers:
1. **The Core Worker (`straddle_worker.py`)**: Runs invisibly on the server. Binds to Postgres to verify DB holidays, watches Spot strikes, and mathematically pieces together unified "Straddle Pairs" over pure Redis hash layers in real-time.
2. **The Output API (`api.py`)**: Hands the data off to real users. Serves fast REST JSON endpoints for historical snapshots and heavy SSE (Server-Sent-Events) Live updates.

---

## 🚀 Optimization & Scaling 
The system has been heavily modified to be entirely crash-resilient under multi-thousand user loads (10k-50k users expected capacity):
- **Redis Connection Drops & TIME_WAIT**: Uses a heavy persistent shared pool for REST hits (`_SYNC_REDIS_POOL`) solving socket exhaustion instantly.
- **Queue Leaks & SSE "Rage Reloader" Errors**: Applies `asyncio.shield` and queue max limits to prevent dropping background Redis `unsubscribe` actions. If a client disconnects violently, RAM queues are still scrubbed surgically. 
- **In-Memory Streaming (Fan-Out)**: Uses 1 centralized Redis subscriber per market and internally delegates the payload to massive memory hooks via `asyncio.Queue`, bypassing generic TCP bounds.

*Read `SSE_SERVER_TUNING_CHECKLIST.md` if pushing traffic beyond 50,000 requests, to raise raw UNIX boundaries!*

---

## 🛠 Operation & Running

Ensure the environment `.env` correctly targets your Neon DB parameters inside `/straddle_Data_Make`. 

### Automation & Holidays Logic
Every trading day explicitly halts internal operations right before the market reset `09:14:00` to wipe prior memory state automatically. However, **this relies strictly on your central Postgres Holiday mappings**.
- If a date (e.g. today `2026-03-31`) resolves `TRUE` inside your main database `holidays` table... it marks a weekend/off-day. The script will intentionally sleep out the cycle preserving previously valid Friday close data natively for API clients. No hard crashes will occur.

---

## 📡 Consumer Endpoints 

### Rest API
- `GET /straddle/current/{SYMBOL}` 
- `GET /straddle/history/{SYMBOL}?limit={x}`
- `GET /index/current/{SYMBOL}`
*(See `FRONTEND_HANDOFF.md` for strict payload JSON representations)*

### Live Market Stream Endpoints (SSE)
You MUST subscribe via valid HTTP native Web Requests to track real-time ticks: 
- `GET /straddle/stream/{SYMBOL}` Output stream
- `GET /index/stream/{SYMBOL}` Output stream