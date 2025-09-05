FROM python:3:11-slim
WORKDIR /Parlance
COPY requirements.txt . 
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8080