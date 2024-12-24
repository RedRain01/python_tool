from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main():

    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("StockPriceChangeAggregation") \
        .config("spark.jars", "/home/why/workSpace/python/data/mysql-connector-j-8.3.0.jar") \
        .config("spark.sql.shuffle.partitions", 200) \
        .getOrCreate()

    # 加载数据
    ticket_date_df = spark.read.format("jdbc").options(
        url="jdbc:mysql://127.0.0.1:9030/demo",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="ticket_date",
        user="root",
        password="why123"
    ).load()

    ticket_df = spark.read.format("jdbc").options(
        url="jdbc:mysql://127.0.0.1:9030/demo",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="ticket_test",
        user="root",
        password="why123"
    ).load()

    # # 检查列名
    # print("Schema of ticket_date_df:")
    # ticket_date_df.printSchema()
    # print("Schema of ticket_df:")
    # ticket_df.printSchema()
    #
    # # 添加别名避免冲突
    # ticket_date_df = ticket_date_df.alias("td")
    # ticket_df = ticket_df.alias("t")
    #
    # # 数据类型检查
    # ticket_date_df = ticket_date_df.withColumn("order_code", F.col("order_code").cast("string"))
    # ticket_df = ticket_df.withColumn("order_code", F.col("order_code").cast("string"))

    # 根据 ticket_date 过滤交易数据
    filtered_tickets_df = ticket_date_df.join(
        ticket_df,
        (ticket_date_df.order_code == ticket_df.order_code) &
        (ticket_date_df.order_date == ticket_df.order_date),
        how="inner"
    ).drop("ticket_date_df.order_code", "ticket_date_df.order_date")

    filtered_tickets_df = filtered_tickets_df.select(ticket_df["*"])

    # 按 time 排序，并计算价格变化
    # filtered_tickets_df.show()
    # result_df = filtered_tickets_df.withColumn(
    #     "prev_price",
    #     F.lag("price").over(window_spec)  # 获取前一条价格
    # ).withColumn(
    #     "price_change",
    #     F.when(F.col("price") != F.col("prev_price"), 1).otherwise(0)  # 检测价格变化
    # )
    # result_df.show()

    # 按 time 排序的窗口定义
    window_spec = Window.partitionBy("order_code", "order_date").orderBy("order_time")
                            # 获取上一条记录的价格
    result_df = filtered_tickets_df.withColumn(
        "prev_price",
        F.lag("price").over(window_spec)  # 获取前一条价格
    ).withColumn(
        "price_change",
        F.when(F.col("price") != F.col("prev_price"), 1).otherwise(0)  # 检测价格变化
    )

    window_spec = Window.partitionBy("order_code", "order_date").orderBy("order_time")

    # 添加上一条记录的价格列，并标记价格变化
    result_df = filtered_tickets_df.withColumn(
        "prev_price",
        F.lag("price").over(window_spec)  # 获取前一条价格
    ).withColumn(
        "price_change",
        F.when(F.col("price") != F.col("prev_price"), 1).otherwise(0)  # 检测价格变化
    )

    # 添加累积 ID，用于标记价格变化段
    result_df = result_df.withColumn(
        "price_segment_id",
        F.sum("price_change").over(window_spec)  # 累积价格变化次数，标记价格段
    )

    result_df.show()
    # 对每个价格段计算 volume 总和和 sum 值
    price_change_records = result_df.groupBy(
        "order_code", "order_date","order_time", "order_dc","prev_price", "price_segment_id", "price"
    ).agg(
        F.sum("volume").alias("volume_sum"),  # 每个段内的 volume 总和
        F.sum(F.col("volume") * F.col("price")).alias("sum")  # 每个段的 sum = volume * price 的总和
    ).select(
        "order_code",
        "order_date",
        "order_time",
        "order_dc",
        "prev_price",
        "price",
        "volume_sum",
        "sum"
    )

    price_change_records.show()


    price_change_records.write.format("jdbc").options(
        url="jdbc:mysql://127.0.0.1:9030/demo",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="ticket_changes_new",
        user="root",
        password="why123"
    ).mode("append").save()



    # 停止 SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
