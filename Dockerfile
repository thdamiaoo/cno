FROM python:3.11.1

WORKDIR /app
ADD ./dags /app
RUN apt update

RUN apt install -y tzdata xvfb python3-tk python3-dev gdebi nano vim gcc libsnappy-dev
RUN pip install --upgrade setuptools
RUN pip install --upgrade pip
RUN pip install pandas sqlalchemy psycopg2-binary python-snappy numpy pyyaml

# Definir diretório de trabalho
WORKDIR /app

# Copiar arquivos do projeto
COPY . /app

# Comando para rodar o script de ETL
# CMD ["python", "script_etl.py"]
