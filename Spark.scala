import org.apache.spark.sql.DataFrame

spark.sparkContext.setCheckpointDir("/tmp/data/output/checkpoints")
// remove broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

// set small number of shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "6")

def duplicateDataset(times: Int): DataFrame => DataFrame = df => {
    Stream.fill(times)(df).reduce(_ union _)
}

def randomSaltJoin(rightDf: DataFrame,
                   joinKeys: Seq[String],
                   seed: Long,
                   saltFactor: Int = 2,
                   saltColName: String = "salt_col"): DataFrame => DataFrame = {
    df =>
        df
        .withColumn(saltColName, round(rand(seed) * saltFactor))
        .join(
            rightDf.withColumn(saltColName, explode(array((0 to saltFactor).map(lit):_*))),
            joinKeys :+ saltColName
        )
        .drop(saltColName)
}

val dfOrderItems = {
    spark
        .read
        .option("header","true")
        .csv("/tmp/data/olist_order_items_dataset.csv")
}
val dfProducts = {
    spark
        .read
        .option("header","true")
        .csv("/tmp/data/olist_products_dataset.csv")
}

// check how skewed the data is on product_id
val dfCountProdId = {
    dfOrderItems
        .groupBy("product_id")
        .count()
}

dfCountProdId.show

// SortMergeJoin
dfOrderItems.join(dfProducts, Seq("product_id")).explain

// HashShuffleJoin
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1")
spark.conf.set("spark.sql.join.preferSortMergeJoin", false)
spark.conf.set("spark.sql.shuffle.partitions", 200)
dfOrderItems.join(dfProducts, Seq("product_id")).explain
spark.conf.set("spark.sql.join.preferSortMergeJoin", true)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

// Amplifying skew
val dfOrderItemsSkew = {
    dfOrderItems
        .withColumn(
            "product_id",
            when(
                rand(1) <= 0.9,
                lit("4244733e06e7ecb4970a6e2683c13e61")
            ).otherwise(col("product_id"))
        )
        .transform(duplicateDataset(15))
}

// salted join
dfOrderItemsSkew.transform(randomSaltJoin(dfProducts, Seq("product_id"), seed=10L, saltFactor=3)).show

// showing what salted join does
dfProducts.where(col("product_id") === "4244733e06e7ecb4970a6e2683c13e61").withColumn("salt_col", round(rand(10L) * 3)).select("product_id", "salt_col").show(truncate=false)
dfOrderItemsSkew.where(col("product_id") === "4244733e06e7ecb4970a6e2683c13e61").withColumn("salt_col", explode(array((0 to 3).map(lit):_*))).select("product_id", "salt_col", "order_id").show(truncate=false)

dfOrderItemsSkew
  .where(col("product_id") === "4244733e06e7ecb4970a6e2683c13e61")
  .withColumn("salt_col", round(rand(10L) * 3)).select("product_id", "salt_col", "order_id")
  .show(truncate=false)