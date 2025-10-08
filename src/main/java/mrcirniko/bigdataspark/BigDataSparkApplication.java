package mrcirniko.bigdataspark;

import org.apache.spark.sql.SparkSession;

import lombok.extern.slf4j.Slf4j;
import mrcirniko.bigdataspark.etl.MockDataToStar;
import mrcirniko.bigdataspark.etl.StarToClickHouse;
import mrcirniko.bigdataspark.etl.StarToMongo;

@Slf4j
public class BigDataSparkApplication {
    public static void main(String[] args) {

        log.info("Starting ETL Application");
        SparkSession spark = SparkSession.builder()
            .appName("ETL Application")
            .getOrCreate();
        
        MockDataToStar.run(spark);
        StarToClickHouse.run(spark);
        StarToMongo.run(spark);

        spark.stop();
    }
}
