FROM gcr.io/nipun-950-core/nipun-base:latest
MAINTAINER david.sargent@nipuncapital.com

RUN yum install -y libffi libffi-devel
ADD requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

ADD np-research.json /tmp/np-research.json
ADD keys /etc/ssl_keys

ENV PYTHONPATH /cloudtools/nipun/
ENV SYS_ENVIRON GCE
ENV ALPHADIR /tmp
ENV RESDIR /tmp
ENV BARRADIR /tmp
ENV UNIVDIR /tmp
ENV PREALPHADIR /tmp
ENV USERDIR /tmp
ENV GOOGLE_APPLICATION_CREDENTIALS /tmp/np-research.json
ENV MYSQLPWD 96nsV18t%wEb##az
ENV MYSQLUSER cjs-user

WORKDIR /cloudtools
ADD cloudtools/ /cloudtools
