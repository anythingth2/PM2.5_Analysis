FROM python:3.7.6-slim

WORKDIR /usr/src/app
RUN mkdir -p /usr/src/app

COPY requirements.txt /usr/src/app/
RUN pip install -r requirements.txt

COPY . /usr/src/app
CMD ["python", "service.py"]
