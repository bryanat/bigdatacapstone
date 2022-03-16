-- Yash's queries
-- All failed transactions by city, country
create or replace view ft_l as (
select order_id, payment_txn_id, product_name, price, product_category, city, country 
from hivetable 
where payment_txn_success = 'N' 
order by country asc, city asc);

-- failed transactions by price
create or replace view ft_p as (
select order_id, payment_txn_id, product_name, price 
from hivetable 
where payment_txn_success = 'N' 
order by price desc);

-- count of failed transactions by product category
create or replace view ft_c as (
select product_category, count(payment_txn_id) as quantity
from hivetable 
where payment_txn_success = 'N' 
group by product_category
order by quantity desc);

-- count of failed transactions by failure reason
create or replace view fr_count as (
select failure_reason, count(payment_txn_id) as quantity 
from hivetable 
where payment_txn_success = 'N' 
group by failure_reason
order by quantity desc);

-- count of successful and failed transactions
create or replace view sVf as (
select payment_txn_success, count(payment_txn_id) as quantity 
from hivetable 
group by payment_txn_success);


-- Brandon Queries
-- Most Common payment Methods 
create or replace view common_payment as (
SELECT payment_type, COUNT(payment_txn_id) as count 
FROM hivetable 
GROUP BY payment_type 
ORDER BY count DESC);

-- Avg purchase cost per person, grouped by ecommerce site
create or replace view avg_cost_ecommerce as (
SELECT ecommerce_website_name, round(AVG(price),2) as avg_price_person 
FROM hivetable 
WHERE payment_txn_success = 'Y' 
GROUP BY ecommerce_website_name 
ORDER BY avg_price_person desc);


-- Queries Mandeep--
-- Display popular product categories and average price per product categories group by product_category
create or replace view pop_cat_avg_price as (
select product_category, MAX(product_count), AVG_price 
from (
	SELECT extract(MONTH from datetime) as month, extract(YEAR from datetime) as year, product_category, COUNT(product_category) as product_count,round(AVG(price),2) as AVG_price 
    FROM hivetable
    WHERE payment_txn_success = 'Y' 
    GROUP BY month, year, product_category
    ) as hivetable1
group by product_category, AVG_price 
order by MAX(product_count) DESC, AVG_price DESC);

 -- Most Popular Dates For Purchases--
  create or replace view pop_date as(SELECT datetime,Count(*) 
     from hivetable 
     WHERE payment_txn_success = 'Y' 
     GROUP BY datetime ORDER BY Count(*) DESC LIMIT 10);
      
-- Queries Ryan Y
-- Change in price for products by timeframe
create or replace view prod_price_change as (
select product_name, price as sale, datetime
from hivetable 
group by product_name, price, datetime 
order by product_name asc, datetime asc);

-- Popular cities/countries by most purchases
create or replace view pop_cities as (
select city, country, count(payment_txn_id) as purchase_count 
from hivetable 
where payment_txn_success = 'Y' 
group by city 
order by purchase_count desc, city asc, country asc);

create or replace view pop_countries as (
select country, count(payment_txn_id) as purchase_count 
from hivetable 
where payment_txn_success = 'Y' 
group by country 
order by purchase_count desc);
