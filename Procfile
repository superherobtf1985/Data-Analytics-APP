worker: celery -A app.celery worker --loglevel=info
web: gunicorn app:app --timeout 120 --log-file -
