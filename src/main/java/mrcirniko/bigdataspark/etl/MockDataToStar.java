package mrcirniko.bigdataspark.etl;

import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.Column;
import static org.apache.spark.sql.functions.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MockDataToStar {

    public static void run(SparkSession spark) {
        log.info("Starting MockData to Star ETL");

        String pgHost = System.getenv().getOrDefault("POSTGRES_HOST", "postgres");
        String pgPort = System.getenv().getOrDefault("POSTGRES_PORT", "5432");
        String pgDb   = System.getenv().getOrDefault("POSTGRES_DB", "dbdb");
        String pgUser = System.getenv().getOrDefault("POSTGRES_USER", "postgres");
        String pgPass = System.getenv().getOrDefault("POSTGRES_PASSWORD", "123456");

        String pgUrl = String.format("jdbc:postgresql://%s:%s/%s", pgHost, pgPort, pgDb);

        Properties pgProps = new Properties();
        pgProps.setProperty("user", pgUser);
        pgProps.setProperty("password", pgPass);
        pgProps.setProperty("driver", "org.postgresql.Driver");

        // 1) Читаем staging как есть
        Dataset<Row> stgRaw = spark.read()
                .format("jdbc")
                .option("url", pgUrl)
                .option("dbtable", "mock_data")
                .option("user", pgProps.getProperty("user"))
                .option("password", pgProps.getProperty("password"))
                .option("driver", pgProps.getProperty("driver"))
                .load();

        // 2) Нормализуем типы (даты и числа)
        Dataset<Row> stg = stgRaw
                // даты -> DateType
                .withColumn("sale_date",            toDateAny(col("sale_date")))
                .withColumn("product_release_date", toDateAny(col("product_release_date")))
                .withColumn("product_expiry_date",  toDateAny(col("product_expiry_date")))
                // числа -> numeric/int/decimal
                .withColumn("product_weight",   col("product_weight").cast("decimal(10,2)"))
                .withColumn("product_rating",   col("product_rating").cast("decimal(10,2)"))
                .withColumn("product_reviews",  col("product_reviews").cast("int"))
                .withColumn("product_price",    col("product_price").cast("decimal(18,2)"))
                .withColumn("sale_quantity",    col("sale_quantity").cast("int"))
                .withColumn("sale_total_price", col("sale_total_price").cast("decimal(18,2)"));

        // 3) Dimensions
        loadDim(stg,
                "sale_customer_id",
                "sale_date",
                new String[]{"customer_first_name","customer_last_name","customer_age","customer_email","customer_country","customer_postal_code"},
                new String[][]{
                        {"sale_customer_id","customer_id"},
                        {"customer_first_name","first_name"},
                        {"customer_last_name","last_name"},
                        {"customer_age","age"},
                        {"customer_email","email"},
                        {"customer_country","country"},
                        {"customer_postal_code","postal_code"}
                },
                pgUrl, pgProps, "dim_customer");

        loadDim(stg,
                "sale_seller_id",
                "sale_date",
                new String[]{"seller_first_name","seller_last_name","seller_email","seller_country","seller_postal_code"},
                new String[][]{
                        {"sale_seller_id","seller_id"},
                        {"seller_first_name","first_name"},
                        {"seller_last_name","last_name"},
                        {"seller_email","email"},
                        {"seller_country","country"},
                        {"seller_postal_code","postal_code"}
                },
                pgUrl, pgProps, "dim_seller");

        loadDim(stg,
                "sale_product_id",
                "sale_date",
                new String[]{"product_name","product_category","product_weight","product_color","product_size","product_brand",
                        "product_material","product_description","product_rating","product_reviews","product_release_date",
                        "product_expiry_date","product_price"},
                new String[][]{
                        {"sale_product_id","product_id"},
                        {"product_name","name"},
                        {"product_category","category"},
                        {"product_weight","weight"},
                        {"product_color","color"},
                        {"product_size","size"},
                        {"product_brand","brand"},
                        {"product_material","material"},
                        {"product_description","description"},
                        {"product_rating","rating"},
                        {"product_reviews","reviews"},
                        {"product_release_date","release_date"},
                        {"product_expiry_date","expiry_date"},
                        {"product_price","unit_price"}
                },
                pgUrl, pgProps, "dim_product");

        loadDim(stg,
                "store_name",
                "sale_date",
                new String[]{"store_location","store_city","store_state","store_country","store_phone","store_email"},
                new String[][]{
                        {"store_name","name"},
                        {"store_location","location"},
                        {"store_city","city"},
                        {"store_state","state"},
                        {"store_country","country"},
                        {"store_phone","phone"},
                        {"store_email","email"}
                },
                pgUrl, pgProps, "dim_store");

        loadDim(stg,
                "supplier_name",
                "sale_date",
                new String[]{"supplier_contact","supplier_email","supplier_phone","supplier_address","supplier_city","supplier_country"},
                new String[][]{
                        {"supplier_name","name"},
                        {"supplier_contact","contact"},
                        {"supplier_email","email"},
                        {"supplier_phone","phone"},
                        {"supplier_address","address"},
                        {"supplier_city","city"},
                        {"supplier_country","country"}
                },
                pgUrl, pgProps, "dim_supplier");

        // 4) dim_date (sale_date уже DateType)
        Dataset<Row> dimDates = stg.select(col("sale_date"))
                .filter(col("sale_date").isNotNull())
                .distinct()
                .withColumn("year",    year(col("sale_date")))
                .withColumn("quarter", quarter(col("sale_date")))
                .withColumn("month",   month(col("sale_date")))
                .withColumn("day",     dayofmonth(col("sale_date")))
                .withColumn("weekday", dayofweek(col("sale_date")));

        dimDates.write().mode(SaveMode.Append).jdbc(pgUrl, "dim_date", pgProps);

        // 5) Читаем dims для join’ов (surrogate keys)
        Dataset<Row> dim_c   = spark.read().jdbc(pgUrl, "dim_customer", pgProps);
        Dataset<Row> dim_s   = spark.read().jdbc(pgUrl, "dim_seller",   pgProps);
        Dataset<Row> dim_p   = spark.read().jdbc(pgUrl, "dim_product",  pgProps);
        Dataset<Row> dim_st  = spark.read().jdbc(pgUrl, "dim_store",    pgProps);
        Dataset<Row> dim_sup = spark.read().jdbc(pgUrl, "dim_supplier", pgProps);
        Dataset<Row> dim_d   = spark.read().jdbc(pgUrl, "dim_date",     pgProps);

        // 6) fact_sales
        Dataset<Row> fact = stg
                .join(dim_d,   stg.col("sale_date").equalTo(dim_d.col("sale_date")))
                .join(dim_c,   stg.col("sale_customer_id").equalTo(dim_c.col("customer_id")))
                .join(dim_s,   stg.col("sale_seller_id").equalTo(dim_s.col("seller_id")))
                .join(dim_p,   stg.col("sale_product_id").equalTo(dim_p.col("product_id")))
                .join(dim_st,  stg.col("store_name").equalTo(dim_st.col("name")))
                .join(dim_sup, stg.col("supplier_name").equalTo(dim_sup.col("name")))
                .select(
                        dim_d.col("date_sk").alias("date_sk"),
                        dim_c.col("customer_sk").alias("customer_sk"),
                        dim_s.col("seller_sk").alias("seller_sk"),
                        dim_p.col("product_sk").alias("product_sk"),
                        dim_st.col("store_sk").alias("store_sk"),
                        dim_sup.col("supplier_sk").alias("supplier_sk"),
                        stg.col("sale_quantity"),
                        stg.col("sale_total_price"),
                        coalesce(stg.col("product_price"), lit(null)).alias("unit_price")
                );

        fact.write().mode(SaveMode.Append).jdbc(pgUrl, "fact_sales", pgProps);

        log.info("MockData to Star ETL completed");
    }

    // helper: строка с датой в одном из форматов -> Column(DateType)
    private static Column toDateAny(Column c) {
        return coalesce(
                to_date(c, "M/d/yyyy"),
                to_date(c, "yyyy-MM-dd")
        );
    }

    private static void loadDim(Dataset<Row> df,
                                String partitionCol,
                                String orderCol,
                                String[] selects,
                                String[][] renames,
                                String pgUrl,
                                Properties pgProps,
                                String targetTable) {
        log.info("Loading dim table {}", targetTable);

        List<Column> cols = new ArrayList<>();
        cols.add(col(partitionCol));
        cols.add(col(orderCol));
        for (String s : selects) cols.add(col(s));

        WindowSpec win = Window.partitionBy(col(partitionCol)).orderBy(col(orderCol));

        Dataset<Row> dfDim = df.select(cols.toArray(new Column[0]))
                .withColumn("rn", row_number().over(win))
                .filter(col("rn").equalTo(lit(1)))
                .drop("rn")
                .drop(orderCol);

        for (String[] pair : renames) {
            dfDim = dfDim.withColumnRenamed(pair[0], pair[1]);
        }

        dfDim.write().mode(SaveMode.Append).jdbc(pgUrl, targetTable, pgProps);
        log.info("Dim table {} loaded", targetTable);
    }
}
