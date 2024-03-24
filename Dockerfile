FROM python:3.9

WORKDIR /app

COPY main.py classes.py utils.py state_utils.py requirements.txt constants.py .env /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
