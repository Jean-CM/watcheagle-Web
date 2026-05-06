import os
import sys
import subprocess
import threading
from datetime import datetime
from flask import redirect, request
from config import JOB_LOG_DIR

os.makedirs(JOB_LOG_DIR, exist_ok=True)


def start_logged_job(script_name, job_name, extra_env=None):
    log_path = os.path.join(JOB_LOG_DIR, f"{job_name}.log")

    def task():
        env = os.environ.copy()
        if extra_env:
            env.update(extra_env)

        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"JOB: {job_name}\n")
            f.write(f"STARTED UTC: {datetime.utcnow()}\n")
            f.write("\n==================== OUTPUT ====================\n\n")
            result = subprocess.run(
                [sys.executable, "-u", os.path.join(os.getcwd(), script_name)],
                stdout=f,
                stderr=f,
                text=True,
                env=env,
            )
            f.write(f"\nRETURN CODE: {result.returncode}\n")

    threading.Thread(target=task, daemon=True).start()


def register_job_routes(app):
    @app.route('/collect-now')
    def collect_now():
        start_logged_job('collect_scrobbles.py', 'collect-now')
        return redirect('/job-log?job=collect-now')

    @app.route('/collect-all')
    def collect_all():
        start_logged_job('backfill_scrobbles.py', 'collect-all')
        return redirect('/job-log?job=collect-all')

    @app.route('/collect-all-selected', methods=['POST'])
    def collect_all_selected():
        team_ids = request.form.getlist('team_ids')
        if not team_ids:
            return 'No seleccionaste equipos', 400

        ids_text = ','.join(team_ids)
        start_logged_job(
            'backfill_scrobbles.py',
            'collect-all-selected',
            extra_env={'TEAM_IDS': ids_text}
        )
        return redirect('/job-log?job=collect-all-selected')

    @app.route('/job-log')
    def job_log():
        job = request.args.get('job', 'collect-now')
        log_path = os.path.join(JOB_LOG_DIR, f'{job}.log')

        if not os.path.exists(log_path):
            content = 'Esperando logs...'
        else:
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

        return f'<pre>{content}</pre>'
