FROM python:2

RUN pip install elasticsearch

RUN pip install elasticsearch-curator

RUN pip install curator

RUN pip install schedule

RUN pip install flask_restful

RUN pip install flask

ADD elasticsearchCurator.py /

CMD ["python", "./elasticsearchCurator.py"]