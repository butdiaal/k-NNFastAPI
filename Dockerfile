FROM python:3.9.13

WORKDIR /app

COPY . .

ENV PYTHONPATH="${PYTHONPATH}:/app"

RUN pip install -r requirements.txt

CMD ["python", "app/main.py"]