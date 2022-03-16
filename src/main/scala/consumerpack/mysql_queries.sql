create or replace view ft as (select order_id, payment_txn_id, product_name, price, product_category, city, country from hivetable where payment_txn_success = 'N');
create or replace view ft_l as (select order_id, payment_txn_id, product_name, price, city, country from hivetable where payment_txn_success = 'N' order by city desc, country desc);
create or replace view ft_p as (select order_id, payment_txn_id, product_name, price from hivetable where payment_txn_success = 'N' order by price desc);
create or replace view ft_c as (select order_id, payment_txn_id, product_name, product_category from hivetable where payment_txn_success = 'N' order by product_category desc);
create or replace view fr_count as (select failure_reason, count(payment_txn_id) as quantity from hivetable where payment_txn_success = 'N' group by failure_reason);
create or replace view sVf as (select payment_txn_success, count(payment_txn_id) as quantity from hivetable group by payment_txn_success);
--Brandon Queries
--Most Common payment Methods 
--SELECT payment_type, COUNT(payment_txn_id) FROM ??? GROUP BY payment_type ORDER BY COUNT(payment_txn_id) DESC
df.groupBy("payment_type").count().orderBy(col("count")).withColumnRenamed("count", "payment_method").show(999)
--Avg purchase cost per person, grouped by ecommerce site
--SELECT AVG(price), customer_id, ecommerce_website_name FROM ??? WHERE payment_txn_success = 'Y' GROUP BY customer_id, ecommerce_website_name
val df_price = df.withColumn("price", round(col("price"), 2))
df_price.where(df("payment_txn_success") === "Y").groupBy("ecommerce_website_name").avg("price").withColumnRenamed("avg(price)", "cost_per_person").orderBy(col("cost_per_person")).show(999)

--Queries Mandeep--
--Display popular product categories and average price per product categories group by product_category--
select month,year,product_category,MAX(product_count),AVG_price from 
    (SELECT extract(MONTH from datetime)as month,extract(YEAR from datetime)as year,product_category,
    COUNT(product_category) as product_count,round(AVG(price),2) as AVG_price from 
     hivetable WHERE payment_txn_success = 'Y' GROUP BY month,year,product_category)as hivetable1
     group by month,year,product_category,AVG_price order by MAX(product_count) DESC;
 --Most Popular Dates For Purchases--    
SELECT extract(DAY from datetime)as day,extract(MONTH from datetime)as month,extract(YEAR from datetime)as year,Count(*) from hivetable WHERE payment_txn_success = 'Y' 
GROUP BY day,month,year ORDER BY Count(*) DESC LIMIT 10;
