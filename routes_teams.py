from flask import redirect, request
from helpers import get_conn


def register_team_routes(app):

    @app.route('/seed-team')
    def seed_team():
        name = request.args.get('name')
        app_name = request.args.get('app')
        user = request.args.get('user')

        conn = get_conn()
        cur = conn.cursor()

        cur.execute('''
            INSERT INTO teams(name, app_name, lastfm_user)
            VALUES(%s,%s,%s)
            ON CONFLICT(lastfm_user)
            DO NOTHING
        ''', (name, app_name, user))

        conn.commit()
        cur.close()
        conn.close()

        return redirect('/?view=monitor')

    @app.route('/delete-team')
    def delete_team():
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            'DELETE FROM teams WHERE id=%s',
            (request.args.get('id'),)
        )

        conn.commit()
        cur.close()
        conn.close()

        return redirect('/?view=monitor')
