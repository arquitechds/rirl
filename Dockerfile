FROM python:3.7.12
ENV PYTHONUNBUFFERED 1
ENV TZ=America/Mexico_City
COPY . /
RUN pip install --upgrade pip && pip install -r requirements.txt
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
CMD python main.py
