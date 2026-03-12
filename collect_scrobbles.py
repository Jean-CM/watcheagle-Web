def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # Teams
    cur.execute("""
    CREATE TABLE IF NOT EXISTS teams (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        app_name VARCHAR(50) NOT NULL,
        lastfm_user VARCHAR(100) NOT NULL UNIQUE,
        status VARCHAR(20) DEFAULT 'PENDING',
        last_scrobble_at TIMESTAMP NULL,
        last_check_at TIMESTAMP NULL,
        idle_minutes INTEGER DEFAULT 0,
        last_alert_at TIMESTAMP NULL,
        active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Scrobbles base
    cur.execute("""
    CREATE TABLE IF NOT EXISTS scrobbles (
        id SERIAL PRIMARY KEY,
        team_id INTEGER REFERENCES teams(id) ON DELETE CASCADE,
        team_name VARCHAR(100),
        lastfm_user VARCHAR(100),
        app_name VARCHAR(50),
        artist VARCHAR(255),
        track VARCHAR(255),
        album VARCHAR(255),
        scrobbled_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Renombrar columnas viejas si existen
    cur.execute("""
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='artist_name'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='artist'
        ) THEN
            ALTER TABLE scrobbles RENAME COLUMN artist_name TO artist;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='track_name'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='track'
        ) THEN
            ALTER TABLE scrobbles RENAME COLUMN track_name TO track;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='album_name'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='album'
        ) THEN
            ALTER TABLE scrobbles RENAME COLUMN album_name TO album;
        END IF;
    END $$;
    """)

    # Agregar columnas faltantes
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS team_id INTEGER;")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS team_name VARCHAR(100);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS lastfm_user VARCHAR(100);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS app_name VARCHAR(50);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS artist VARCHAR(255);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS track VARCHAR(255);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS album VARCHAR(255);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS scrobbled_at TIMESTAMP;")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")

    # Quitar NOT NULL heredado si venía de esquema viejo
    cur.execute("ALTER TABLE scrobbles ALTER COLUMN artist DROP NOT NULL;")
    cur.execute("ALTER TABLE scrobbles ALTER COLUMN track DROP NOT NULL;")
    cur.execute("ALTER TABLE scrobbles ALTER COLUMN album DROP NOT NULL;")

    # Índice único dedupe
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_scrobbles_unique
    ON scrobbles (lastfm_user, artist, track, scrobbled_at);
    """)

    conn.commit()
    cur.close()
    conn.close()
