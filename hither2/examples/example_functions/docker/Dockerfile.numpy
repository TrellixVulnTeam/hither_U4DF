FROM python:3.8

RUN pip install simplejson requests click
RUN pip install numpy

LABEL version="0.1.0"