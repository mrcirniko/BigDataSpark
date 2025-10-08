package mrcirniko.bigdataspark.etl;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.SaveMode;
import static org.apache.spark.sql.functions.*;


import java.util.Properties;

@Slf4j
public class StarToMongo {

    public static void run(SparkSession spark) {
        log.info("Starting Star to MongoDB ETL");

        String pgHost = System.getenv().getOrDefault("POSTGRES_HOST", "postgres");
        String pgPort = System.getenv().getOrDefault("POSTGRES_PORT", "5432");
        String pgDb   = System.getenv().getOrDefault("POSTGRES_DB", "dbdb");
        String pgUser = System.getenv().getOrDefault("POSTGRES_USER", "postgres");
        String pgPass = System.getenv().getOrDefault("POSTGRES_PASSWORD", "123456");
        String pgUrl  = String.format("jdbc:postgresql://%s:%s/%s", pgHost, pgPort, pgDb);

        Properties pgProps = new Properties();
        pgProps.setProperty("user", pgUser);
        pgProps.setProperty("password", pgPass);
        pgProps.setProperty("driver", "org.postgresql.Driver");

        Dataset<Row> fact       = spark.read().jdbc(pgUrl, "fact_sales",   pgProps);
        Dataset<Row> dimProduct = spark.read().jdbc(pgUrl, "dim_product",  pgProps);
        Dataset<Row> dimCustomer= spark.read().jdbc(pgUrl, "dim_customer", pgProps);
        Dataset<Row> dimDate    = spark.read().jdbc(pgUrl, "dim_date",     pgProps);
        Dataset<Row> dimStore   = spark.read().jdbc(pgUrl, "dim_store",    pgProps);
        Dataset<Row> dimSupplier= spark.read().jdbc(pgUrl, "dim_supplier", pgProps);

        Dataset<Row> sales = fact
            .join(dimProduct, fact.col("product_sk").equalTo(dimProduct.col("product_sk")))
            .join(dimCustomer, fact.col("customer_sk").equalTo(dimCustomer.col("customer_sk")))
            .join(dimDate,     fact.col("date_sk").equalTo(dimDate.col("date_sk")))
            .join(dimStore,    fact.col("store_sk").equalTo(dimStore.col("store_sk")))
            .join(dimSupplier, fact.col("supplier_sk").equalTo(dimSupplier.col("supplier_sk")))
            .select(
                dimProduct.col("product_id"),
                dimProduct.col("name").alias("product_name"),
                dimProduct.col("category"),
                dimProduct.col("rating"),
                dimProduct.col("reviews"),
                dimCustomer.col("customer_id"),
                dimCustomer.col("first_name").alias("customer_first_name"),
                dimCustomer.col("last_name").alias("customer_last_name"),
                dimCustomer.col("country").alias("customer_country"),
                dimStore.col("name").alias("store_name"),
                dimStore.col("city").alias("store_city"),
                dimStore.col("country").alias("store_country"),
                dimSupplier.col("name").alias("supplier_name"),
                dimSupplier.col("country").alias("supplier_country"),
                dimDate.col("year"),
                dimDate.col("month"),
                fact.col("sale_quantity"),
                fact.col("sale_total_price"),
                fact.col("unit_price")
            )
            .withColumn("amount",
                when(col("sale_total_price").isNotNull(), col("sale_total_price"))
                .otherwise( col("unit_price").multiply(col("sale_quantity")) )
            );

        String mongoUri = System.getenv().getOrDefault("MONGO_URI", "mongodb://mongodb:27017");
        String mongoDb  = System.getenv().getOrDefault("MONGO_DB", "reports");

        java.util.function.BiConsumer<Dataset<Row>, String> saveMongo =
            (df, coll) -> df.write()
                .format("mongodb") // v10+ коннектор
                .option("connection.uri", mongoUri)
                .option("database", mongoDb)
                .option("collection", coll)
                .mode(SaveMode.Overwrite)
                .save();

        Dataset<Row> prodAgg = sales.groupBy("product_id","product_name","category")
            .agg(
                sum("sale_quantity").alias("total_qty"),
                sum("amount").alias("total_revenue"),
                first("rating", true).alias("rating"),
                first("reviews", true).alias("reviews")
            );

        WindowSpec wQtyDesc = Window.orderBy(col("total_qty").desc());
        saveMongo.accept(
            prodAgg.withColumn("rn", row_number().over(wQtyDesc)).filter(col("rn").leq(10)).drop("rn"),
            "product_sales_top10");

        saveMongo.accept(
            sales.groupBy("category").agg(sum("amount").alias("total_revenue")),
            "revenue_by_category");

        saveMongo.accept(
            prodAgg.select("product_id","product_name","rating","reviews"),
            "product_rating_reviews");

        Dataset<Row> custAgg = sales.groupBy("customer_id","customer_first_name","customer_last_name","customer_country")
            .agg(sum("amount").alias("total_amount"),
                 count(lit(1)).alias("orders_cnt"),
                 avg("amount").alias("avg_check"));

        WindowSpec wAmtDesc = Window.orderBy(col("total_amount").desc());
        saveMongo.accept(
            custAgg.withColumn("rn", row_number().over(wAmtDesc)).filter(col("rn").leq(10)).drop("rn"),
            "customer_top10_total_amount");

        saveMongo.accept(
            custAgg.groupBy("customer_country").agg(countDistinct("customer_id").alias("customers")),
            "customers_by_country");

        saveMongo.accept(
            custAgg.select("customer_id","customer_first_name","customer_last_name","avg_check"),
            "customer_avg_check");

        saveMongo.accept(
            sales.groupBy("year","month")
                 .agg(sum("amount").alias("total_revenue"),
                      count(lit(1)).alias("orders_cnt"),
                      avg("amount").alias("avg_order")),
            "time_monthly_trends");

        saveMongo.accept(
            sales.groupBy("year")
                 .agg(sum("amount").alias("total_revenue"),
                      count(lit(1)).alias("orders_cnt"),
                      avg("amount").alias("avg_order")),
            "time_yearly_trends");

        Dataset<Row> storeAgg = sales.groupBy("store_name","store_city","store_country")
            .agg(sum("amount").alias("total_revenue"),
                 count(lit(1)).alias("orders_cnt"),
                 avg("amount").alias("avg_check"));

        WindowSpec wStore = Window.orderBy(col("total_revenue").desc());
        saveMongo.accept(
            storeAgg.withColumn("rn", row_number().over(wStore)).filter(col("rn").leq(5)).drop("rn"),
            "store_top5_revenue");

        saveMongo.accept(
            storeAgg.groupBy("store_city","store_country")
                    .agg(sum("total_revenue").alias("total_revenue"),
                         sum("orders_cnt").alias("orders_cnt")),
            "sales_by_city_country");

        saveMongo.accept(
            storeAgg.select("store_name","avg_check"),
            "store_avg_check");

        Dataset<Row> supplierAgg = sales.groupBy("supplier_name","supplier_country")
            .agg(sum("amount").alias("total_revenue"),
                 avg("unit_price").alias("avg_unit_price"),
                 count(lit(1)).alias("orders_cnt"));

        WindowSpec wSup = Window.orderBy(col("total_revenue").desc());
        saveMongo.accept(
            supplierAgg.withColumn("rn", row_number().over(wSup)).filter(col("rn").leq(5)).drop("rn"),
            "supplier_top5_revenue");

        saveMongo.accept(
            supplierAgg.select("supplier_name","avg_unit_price"),
            "supplier_avg_price");

        saveMongo.accept(
            supplierAgg.groupBy("supplier_country")
                       .agg(sum("total_revenue").alias("total_revenue"),
                            sum("orders_cnt").alias("orders_cnt")),
            "supplier_by_country");

        WindowSpec wBest = Window.orderBy(col("rating").desc());
        WindowSpec wWorst= Window.orderBy(col("rating").asc());

        saveMongo.accept(
            prodAgg.withColumn("rn", row_number().over(wBest)).filter(col("rn").leq(10)).drop("rn")
                   .select("product_id","product_name","category","rating","reviews","total_qty","total_revenue"),
            "quality_best_rated_top10");

        saveMongo.accept(
            prodAgg.withColumn("rn", row_number().over(wWorst)).filter(col("rn").leq(10)).drop("rn")
                   .select("product_id","product_name","category","rating","reviews","total_qty","total_revenue"),
            "quality_worst_rated_top10");

        saveMongo.accept(
            prodAgg.orderBy(col("reviews").desc()).limit(10)
                   .select("product_id","product_name","category","reviews","rating","total_qty","total_revenue"),
            "quality_most_reviews_top10");

        saveMongo.accept(
            sales.select(col("rating").cast("double").alias("rating"),
                         col("sale_quantity").cast("double").alias("qty"))
                 .agg(corr(col("rating"), col("qty")).alias("pearson_corr")),
            "quality_rating_sales_corr");

        log.info("Star to MongoDB ETL done");
    }
}
