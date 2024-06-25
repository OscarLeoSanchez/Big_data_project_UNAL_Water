FROM python:3.8-slim


RUN pip install faker pandas geopandas pyarrow shapely osmnx

COPY server.py /app/server.py

WORKDIR /app

EXPOSE 65432

ENTRYPOINT ["python3", "server.py"]

CMD ["--interval", "30", "--port", "65432", "--host", "0.0.0.0","--batch_size","-1", "--initial_date","2010-01-01"]