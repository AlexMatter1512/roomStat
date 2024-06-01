FROM python:3.11

RUN pip install kafka-python pyserial

COPY ./ /producer

CMD ["python", "/producer.py"]