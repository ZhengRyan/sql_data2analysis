#!/usr/bin/env python
# ! -*- coding: utf-8 -*-

'''
@File: 计算N日留存率.py
@Author: RyanZheng
@Email: ryan.zhengrp@gmail.com
@Created Time on: 2022-01-08

这里的留存率的计算逻辑是这样的：在2022-01-01有10个人活跃咯，那么对于2022-01-01的1日留存率和7日留存率分别是这么计算的
2022-01-01的1日留存率：如果2022-01-01的这10个人在2022-01-02在有2个人活跃咯，那么2022-01-01的1日留存率就是：2/10=20%
2022-01-01的7日留存率：如果2022-01-01的这10个人在2022-01-02在有2个人活跃咯，在3日、4日、5日、6日这10个人都没有活跃，在7日时，10人中有4人活跃咯，那么2022-01-01的7日留存率就是：4/10=40%
'''

from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
#spark_df = spark.read.option("header", True).option("inferSchema", True).csv("/Users/ryanzheng/PycharmProjects/sql_data2analysis/计算n日留存率/data/data.csv")
#spark_df = spark.read.option("header", True).option("inferSchema", True).csv("/Users/ryanzheng/PycharmProjects/sql_data2analysis/计算n日留存率/data/data2.csv")
spark_df = spark.read.option("header", True).option("inferSchema", True).csv("/Users/ryanzheng/PycharmProjects/sql_data2analysis/计算n日留存率/data/data3.csv")
spark_df.show(100, False)


spark_df.createOrReplaceTempView("tmp")

# spark.sql("""
# select datediff('2021-01-02','2021-01-01') from tmp
# """).show(100,False)


###主要逻辑
spark.sql("""
select t1.log_time,
        count(case when datediff(t2.log_time,t1.log_time)=1 then t1.user_id end) / count(distinct t1.user_id) as retention_rate_1day,
        count(case when datediff(t2.log_time,t1.log_time)=2 then t1.user_id end) / count(distinct t1.user_id) as retention_rate_2day,
        count(case when datediff(t2.log_time,t1.log_time)=6 then t1.user_id end) / count(distinct t1.user_id) as retention_rate_7day,
        count(case when datediff(t2.log_time,t1.log_time)=1 then t1.user_id end) as t1_count_id_1day,
        count(case when datediff(t2.log_time,t1.log_time)=2 then t1.user_id end) as t1_count_id_2day,
        count(case when datediff(t2.log_time,t1.log_time)=3 then t1.user_id end) as t1_count_id_3day,
        count(case when datediff(t2.log_time,t1.log_time)=4 then t1.user_id end) as t1_count_id_4day,
        count(case when datediff(t2.log_time,t1.log_time)=5 then t1.user_id end) as t1_count_id_5day,
        count(case when datediff(t2.log_time,t1.log_time)=6 then t1.user_id end) as t1_count_id_6day,
        count(case when datediff(t2.log_time,t1.log_time)=7 then t1.user_id end) as t1_count_id_7day,
        count(distinct t1.user_id) as t1_count_id,
        count(distinct t2.user_id) as t2_count_id
from
    (select log_time,user_id
        from tmp
        where log_time between '2022-01-01' and '2022-01-09'
        group by log_time,user_id) t1
    left join
    (select log_time,user_id
        from tmp
        where log_time between '2022-01-01' and '2022-01-08'
        group by log_time,user_id) t2
    on t1.user_id = t2.user_id
    group by t1.log_time

""").show(100, False)
###主要逻辑

#
# spark.sql("""
# select t1.*,t2.*
# from
#     (select log_time as t1_log_time,user_id as t1_user_id
#         from tmp
#         where log_time >= '2022-01-01' and  log_time <= '2022-01-09'
#         group by log_time,user_id) t1
#     left join
#     (select log_time,user_id
#         from tmp
#         where log_time >= '2022-01-01' and log_time <= '2022-01-08'
#         group by log_time,user_id) t2
#     on t1.t1_user_id = t2.user_id
#
# """).repartition(1).write.mode("overwrite").option("header", True).csv("/Users/ryanzheng/PycharmProjects/sql_data2analysis/计算n日留存率/data/data_jjjj3")
