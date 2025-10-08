package mrcirniko.bigdataspark.etl;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.SaveMode;
import static org.apache.spark.sql.functions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import java.util.function.BiConsumer;

@Slf4j
public class StarToClickHouse {

    public static void run(SparkSession spark) {
        log.info("Starting Star to ClickHouse ETL");

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

        Dataset<Row> fact        = spark.read().jdbc(pgUrl, "fact_sales",   pgProps);
        Dataset<Row> dimProduct  = spark.read().jdbc(pgUrl, "dim_product",  pgProps);
        Dataset<Row> dimCustomer = spark.read().jdbc(pgUrl, "dim_customer", pgProps);
        Dataset<Row> dimDate     = spark.read().jdbc(pgUrl, "dim_date",     pgProps);
        Dataset<Row> dimStore    = spark.read().jdbc(pgUrl, "dim_store",    pgProps);
        Dataset<Row> dimSupplier = spark.read().jdbc(pgUrl, "dim_supplier", pgProps);

        Dataset<Row> sales = fact
                .join(dimProduct,  fact.col("product_sk").equalTo(dimProduct.col("product_sk")))
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
                                .otherwise(col("unit_price").multiply(col("sale_quantity")))
                );

        String chHost = System.getenv().getOrDefault("CH_HOST", "clickhouse");
        String chPort = System.getenv().getOrDefault("CH_PORT", "8123");
        String chDb   = System.getenv().getOrDefault("CH_DB", "reports");
        String chUser = System.getenv().getOrDefault("CH_USER", "default");
        String chPass = System.getenv().getOrDefault("CH_PASSWORD", "");
        String chUrl  = String.format("jdbc:clickhouse://%s:%s/%s", chHost, chPort, chDb);

        ensureClickHouseDb(chHost, chPort, chDb, chUser, chPass);

        BiConsumer<Dataset<Row>, String> saveCH = (df, table) -> {
            log.info("Saving table: {}", table);
            df.printSchema();
            df.write()
                    .mode(SaveMode.Overwrite)
                    .format("jdbc")
                    .option("url", chUrl)
                    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                    .option("dbtable", table)
                    .option("createTableOptions", engineOrderByFor(table))
                    .save();
        };

        Dataset<Row> prodAgg = sales.groupBy("product_id","product_name","category")
                .agg(
                        sum("sale_quantity").alias("total_qty"),
                        sum("amount").alias("total_revenue"),
                        first("rating", true).alias("rating"),
                        first("reviews", true).alias("reviews")
                );

        WindowSpec wQtyDesc = Window.orderBy(col("total_qty").desc());
        Dataset<Row> productTop10 = prodAgg.withColumn("rn", row_number().over(wQtyDesc))
                .filter(col("rn").leq(10)).drop("rn");
        saveCH.accept(productTop10, "product_sales_top10");

        Dataset<Row> revenueByCategory = sales.groupBy("category")
                .agg(sum("amount").alias("total_revenue"));
        saveCH.accept(revenueByCategory, "revenue_by_category");

        Dataset<Row> productRatingReviews = prodAgg
                .select("product_id","product_name","rating","reviews");
        saveCH.accept(productRatingReviews, "product_rating_reviews");

        Dataset<Row> custAgg = sales.groupBy("customer_id","customer_first_name",
                        "customer_last_name","customer_country")
                .agg(
                        sum("amount").alias("total_amount"),
                        count(lit(1)).alias("orders_cnt"),
                        avg("amount").alias("avg_check")
                );

        WindowSpec wAmtDesc = Window.orderBy(col("total_amount").desc());
        Dataset<Row> customerTop10 = custAgg.withColumn("rn", row_number().over(wAmtDesc))
                .filter(col("rn").leq(10)).drop("rn");
        saveCH.accept(customerTop10, "customer_top10_total_amount");

        Dataset<Row> customersByCountry = custAgg.groupBy("customer_country")
                .agg(countDistinct("customer_id").alias("customers"));
        saveCH.accept(customersByCountry, "customers_by_country");

        Dataset<Row> customerAvgCheck = custAgg
                .select("customer_id","customer_first_name","customer_last_name","avg_check");
        saveCH.accept(customerAvgCheck, "customer_avg_check");

        Dataset<Row> monthlyTrends = sales.groupBy("year","month")
                .agg(sum("amount").alias("total_revenue"),
                        count(lit(1)).alias("orders_cnt"),
                        avg("amount").alias("avg_order"));
        saveCH.accept(monthlyTrends, "time_monthly_trends");

        Dataset<Row> yearlyTrends = sales.groupBy("year")
                .agg(sum("amount").alias("total_revenue"),
                        count(lit(1)).alias("orders_cnt"),
                        avg("amount").alias("avg_order"));
        saveCH.accept(yearlyTrends, "time_yearly_trends");

        Dataset<Row> storeAgg = sales.groupBy("store_name","store_city","store_country")
                .agg(sum("amount").alias("total_revenue"),
                        count(lit(1)).alias("orders_cnt"),
                        avg("amount").alias("avg_check"));

        WindowSpec wStore = Window.orderBy(col("total_revenue").desc());
        Dataset<Row> storeTop5 = storeAgg.withColumn("rn", row_number().over(wStore))
                .filter(col("rn").leq(5)).drop("rn");
        saveCH.accept(storeTop5, "store_top5_revenue");

        Dataset<Row> salesByCityCountry = storeAgg.groupBy("store_city","store_country")
                .agg(sum("total_revenue").alias("total_revenue"),
                        sum("orders_cnt").alias("orders_cnt"));
        saveCH.accept(salesByCityCountry, "sales_by_city_country");

        Dataset<Row> storeAvgCheck = storeAgg.select("store_name","avg_check");
        saveCH.accept(storeAvgCheck, "store_avg_check");

        Dataset<Row> supplierAgg = sales.groupBy("supplier_name","supplier_country")
                .agg(sum("amount").alias("total_revenue"),
                        avg("unit_price").alias("avg_unit_price"),
                        count(lit(1)).alias("orders_cnt"));

        WindowSpec wSup = Window.orderBy(col("total_revenue").desc());
        Dataset<Row> supplierTop5 = supplierAgg.withColumn("rn", row_number().over(wSup))
                .filter(col("rn").leq(5)).drop("rn");
        saveCH.accept(supplierTop5, "supplier_top5_revenue");

        Dataset<Row> supplierAvgPrice = supplierAgg
                .select("supplier_name","avg_unit_price");
        saveCH.accept(supplierAvgPrice, "supplier_avg_price");

        Dataset<Row> supplierByCountry = supplierAgg.groupBy("supplier_country")
                .agg(sum("total_revenue").alias("total_revenue"),
                        sum("orders_cnt").alias("orders_cnt"));
        saveCH.accept(supplierByCountry, "supplier_by_country");

        WindowSpec wBest = Window.orderBy(col("rating").desc());
        WindowSpec wWorst = Window.orderBy(col("rating").asc());

        Dataset<Row> bestRated = prodAgg.withColumn("rn", row_number().over(wBest))
                .filter(col("rn").leq(10)).drop("rn")
                .select("product_id","product_name","category","rating","reviews","total_qty","total_revenue");
        saveCH.accept(bestRated, "quality_best_rated_top10");

        Dataset<Row> worstRated = prodAgg.withColumn("rn", row_number().over(wWorst))
                .filter(col("rn").leq(10)).drop("rn")
                .select("product_id","product_name","category","rating","reviews","total_qty","total_revenue");
        saveCH.accept(worstRated, "quality_worst_rated_top10");

        Dataset<Row> mostReviews = prodAgg.orderBy(col("reviews").desc()).limit(10)
                .select("product_id","product_name","category","reviews","rating","total_qty","total_revenue");
        saveCH.accept(mostReviews, "quality_most_reviews_top10");

        Dataset<Row> corr = sales
                .select(col("rating").cast("double").alias("rating"),
                        col("sale_quantity").cast("double").alias("qty"))
                .agg(corr(col("rating"), col("qty")).alias("pearson_corr"));
        saveCH.accept(corr, "quality_rating_sales_corr");

        log.info("Star to ClickHouse ETL done");
    }

    private static void ensureClickHouseDb(String host, String port, String db, String user, String pass) {
        String rootUrl = String.format("jdbc:clickhouse://%s:%s", host, port);
        try (Connection c = DriverManager.getConnection(rootUrl, user, pass);
             Statement st = c.createStatement()) {
            st.execute("CREATE DATABASE IF NOT EXISTS " + db);
        } catch (Exception e) {
            throw new RuntimeException("ClickHouse DB init failed", e);
        }
    }

    private static String engineOrderByFor(String table) {
        if (table.equals("product_sales_top10"))       return "ENGINE = MergeTree ORDER BY (product_id)";
        if (table.equals("revenue_by_category"))       return "ENGINE = MergeTree ORDER BY (category)";
        if (table.equals("product_rating_reviews"))    return "ENGINE = MergeTree ORDER BY (product_id)";
        if (table.equals("customer_top10_total_amount")) return "ENGINE = MergeTree ORDER BY (customer_id)";
        if (table.equals("customers_by_country"))      return "ENGINE = MergeTree ORDER BY (customer_country)";
        if (table.equals("customer_avg_check"))        return "ENGINE = MergeTree ORDER BY (customer_id)";
        if (table.equals("time_monthly_trends"))       return "ENGINE = MergeTree ORDER BY (year, month)";
        if (table.equals("time_yearly_trends"))        return "ENGINE = MergeTree ORDER BY (year)";
        if (table.equals("store_top5_revenue"))        return "ENGINE = MergeTree ORDER BY (store_name)";
        if (table.equals("sales_by_city_country"))     return "ENGINE = MergeTree ORDER BY (store_city, store_country)";
        if (table.equals("store_avg_check"))           return "ENGINE = MergeTree ORDER BY (store_name)";
        if (table.equals("supplier_top5_revenue"))     return "ENGINE = MergeTree ORDER BY (supplier_name)";
        if (table.equals("supplier_avg_price"))        return "ENGINE = MergeTree ORDER BY (supplier_name)";
        if (table.equals("supplier_by_country"))       return "ENGINE = MergeTree ORDER BY (supplier_country)";
        if (table.equals("quality_best_rated_top10"))  return "ENGINE = MergeTree ORDER BY (product_id)";
        if (table.equals("quality_worst_rated_top10")) return "ENGINE = MergeTree ORDER BY (product_id)";
        if (table.equals("quality_most_reviews_top10"))return "ENGINE = MergeTree ORDER BY (product_id)";
        if (table.equals("quality_rating_sales_corr")) return "ENGINE = MergeTree ORDER BY tuple()";
        return "ENGINE = MergeTree ORDER BY tuple()";
    }
}
