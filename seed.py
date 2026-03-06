import os
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS teams (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    app_name VARCHAR(50) NOT NULL,
    lastfm_user VARCHAR(100) NOT NULL UNIQUE,
    status VARCHAR(20) DEFAULT 'PENDING',
    last_scrobble_at TIMESTAMP NULL,
    last_check_at TIMESTAMP NULL,
    idle_minutes INTEGER DEFAULT 0,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

cur.execute("""
INSERT INTO teams (name, app_name, lastfm_user, status)
VALUES (%s, %s, %s, %s)
ON CONFLICT (lastfm_user) DO NOTHING;
""", ("equipo01", "spotify", "JeanCMP", "PENDING"))

conn.commit()
cur.close()
conn.close()

print("Seed completado.")
