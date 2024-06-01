FROM spark:python3

COPY ./example.py /opt/spark/work-dir/example.py

# CMD [ "python3", "/opt/spark/work-dir/example.py"]
CMD [ "/opt/spark/bin/spark-submit", "--master", "local", "/opt/spark/work-dir/example.py"]