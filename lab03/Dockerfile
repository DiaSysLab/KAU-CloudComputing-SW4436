FROM python:3.12-alpine
WORKDIR /app
COPY server.py loadgen.py /app/
USER 1000:1000
EXPOSE 8000
CMD ["python", "-u", "/app/server.py"]
