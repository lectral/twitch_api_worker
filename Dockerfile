FROM python:3

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y mariadb-client python3-dev && rm -rf /var/lib/apt

WORKDIR /usr/src/app
COPY . . 

CMD ["./periodic_run.sh"]
