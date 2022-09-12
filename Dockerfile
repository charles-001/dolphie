FROM python:3.9-slim

RUN pip3 install rich pymysql charset_normalizer myloginpath packaging

COPY dolphie /usr/bin/dolphie