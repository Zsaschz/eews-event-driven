FROM python:3.10-slim

ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

ENV PORT 8000
EXPOSE $PORT
RUN pip install --no-cache-dir -r requirements.txt

CMD exec uvicorn rest.main:app --host 0.0.0.0 --port ${PORT} --workers 8