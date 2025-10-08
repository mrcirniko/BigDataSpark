#!/bin/bash
# Полный запуск ETL: сборка, подъем контейнеров и запуск Spark приложения

set -e
set -a
source .env
set +a

echo "==== Building Java project ===="
mvn clean package -DskipTests

echo "==== Starting Docker containers ===="
sudo docker-compose up -d

echo "Waiting for Postgres to be ready..."
until docker exec -i bigdataspark_postgres_1 pg_isready -U "$POSTGRES_USER" > /dev/null 2>&1; do
  sleep 2
done
echo "Postgres is ready."

echo "Waiting for MongoDB to be ready..."
until docker exec -i bigdataspark_mongodb_1 mongosh --quiet --eval 'db.runCommand({ ping: 1 })' >/dev/null 2>&1; do
  sleep 2
done
echo "MongoDB is ready."

echo "Waiting for ClickHouse to be ready..."
until curl -s 'http://localhost:8123/?query=SELECT%201' | grep -q 1; do
  sleep 2
done
echo "ClickHouse is ready."

echo "Waiting for Spark Master to be ready..."
until curl -s http://localhost:8080 > /dev/null 2>&1; do
  sleep 2
done
echo "Spark Master is ready."

echo "Copying JAR to Spark Master..."
docker cp target/etl-to-star-1.0-SNAPSHOT.jar bigdataspark_spark-master_1:/opt/spark-apps/

echo "Running ETL application..."
docker exec -i bigdataspark_spark-master_1 spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --class mrcirniko.bigdataspark.BigDataSparkApplication \
  /opt/spark-apps/etl-to-star-1.0-SNAPSHOT.jar

echo "==== ETL completed ===="
