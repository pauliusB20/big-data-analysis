FROM jupyter/pyspark-notebook:python-3.11

USER root

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt


EXPOSE 8888

CMD [\
  "jupyter", \
  "notebook", \
  "--ip=0.0.0.0", \
  "--port=8888", \
  "--no-browser", \
  "--allow-root"\
]