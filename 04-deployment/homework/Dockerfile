

FROM agrigorev/zoomcamp-model:mlops-3.9.7-slim

RUN pip install -U pip
WORKDIR /app

COPY requirements.txt ./


RUN pip install -r requirements.txt

COPY starter.py ./

ENTRYPOINT [ "python", "starter.py" ]