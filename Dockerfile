FROM python:3.11.1

# Definir diretório de trabalho
WORKDIR /app

# Atualizar pacotes e instalar dependências do sistema
RUN apt-get update && \
    apt-get install -y tzdata xvfb python3-tk python3-dev gdebi nano vim gcc libsnappy-dev postgresql-client

# Atualizar setuptools e pip
RUN pip install --upgrade setuptools pip

# Instalar pacotes Python necessários
RUN pip install pandas sqlalchemy psycopg2-binary python-snappy numpy pyyaml

# Copiar arquivos do projeto para o contêiner
COPY . /app

# Comando para rodar o script de ETL
# CMD ["python", "script_etl.py"]
