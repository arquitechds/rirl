FROM python:3.8
ENV PYTHONUNBUFFERED 1
ENV TZ=America/Mexico_City
COPY . /
RUN pip install --upgrade pip && pip install -r requirements.txt
RUN apt update
#RUN apt install nodejs -y
#RUN apt install npm -y
#RUN npm install --global curlconverter
#RUN npm install curlconverter
#CMD python main.py
