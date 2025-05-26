FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y wget gnupg2 curl openjdk-17-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY . /coord_UK

WORKDIR /coord_UK

RUN pip install --no-cache-dir -r requirements.txt 

CMD [ "python", "src/main.py"]