-- correlations_by_category
create or replace table public.correlations_by_category as
WITH product_sales AS (SELECT p.productid,productsubcategoryid,p.name,OrderQty,p.listprice,s.unitprice FROM 
                       product p 
                       JOIN salesorderdetail s ON (p.productid = s.productid)),
     top_subcategories AS ( SELECT productsubcategoryid, SUM(OrderQty) AS totalSales
                        FROM product_sales
                        GROUP BY productsubcategoryid
                        ORDER BY totalSales DESC
                        LIMIT 10),
    products_topcategories AS (SELECT productsubcategoryid,productid,name, OrderQty,unitprice
                        FROM product_sales
                        WHERE productsubcategoryid IN (SELECT productsubcategoryid FROM top_subcategories)
                              )
                        
SELECT productsubcategoryid, productid,name,corr(OrderQty, unitprice) as corr FROM 
products_topcategories
GROUP BY (productsubcategoryid, productid,name)
ORDER BY corr ASC
LIMIT 10;



-- // top product by sales with desription and name

-- create or replace table public.top_products as

-- WITH top_products_ids AS (SELECT productid, SUM(OrderQty) AS totalSales
--                     from salesorderdetail
--                     GROUP BY productid
--                     ORDER BY totalSales DESC
--                     LIMIT 10)

-- SELECT p.productmodelid as ModelID, name, description, totalsales,getdate() as date_in_top FROM 
-- top_products_ids tp
-- LEFT JOIN product p ON (p.productid = tp.productid)
-- LEFT JOIN productmodelproductdescriptionculture pm ON (pm.productmodelid = p.productmodelid)
-- LEFT JOIN productdescription pd ON (pd.productdescriptionid = pm.productdescriptionid)
-- WHERE pm.cultureid like '%en%'
-- ORDER BY totalSales DESC;


create or replace table public.temp_top_products as
WITH top_products_ids AS (SELECT productid, SUM(OrderQty) AS totalSales
                    from salesorderdetail
                    GROUP BY productid
                    ORDER BY totalSales DESC
                    LIMIT 10)

SELECT p.productmodelid as ModelID, name, description, totalsales,getdate() as date_in_top FROM 
top_products_ids tp
LEFT JOIN product p ON (p.productid = tp.productid)
LEFT JOIN productmodelproductdescriptionculture pm ON (pm.productmodelid = p.productmodelid)
LEFT JOIN productdescription pd ON (pd.productdescriptionid = pm.productdescriptionid)
WHERE pm.cultureid like '%en%'
ORDER BY totalSales DESC;

                    
merge into public.top_products as target using public.temp_top_products as source
on source.ModelID = target.ModelID
when not matched then
    INSERT (ModelID, name, description, totalsales,date_in_top) 
    VALUES (source.ModelID,source.name,source.description,source. totalsales,source.date_in_top);
    
drop table public.temp_top_products;


-- // correlation but correcting correlation

-- create or replace table public.correlation_subtotal_comission_currency_ok as
-- with sales_rate_currency as (SELECT s.orderdate,s.salespersonid,s.currencyrateid, s.subtotal,c.averagerate FROM 
--                      currencyrate c join
--                      salesorderheader s on c.currencyrateid = s.currencyrateid),
--      salesorder_currency AS (SELECT s.salespersonid,s.currencyrateid, s.subtotal,
--                                 (CASE
--                                 WHEN s.currencyrateid IS NULL THEN 1
--                                 ELSE s.averagerate
--                                 END) as averagerate
--                                 FROM 
--                                 sales_rate_currency s
--                                 WHERE s.salespersonid IS NOT NULL
--                                 AND s.orderdate BETWEEN '2014-01-01'::date 
--                                  AND '2014-12-31'::date),
--     salesorder_usd AS (SELECT salespersonid,subtotal*averagerate as subtotal_usd
--                     FROM 
--                     salesorder_currency
--                     ORDER BY subtotal_usd DESC)
-- SELECT corr(su.subtotal_usd, sp.commissionpct) as correlation,GETDATE() as date FROM 
-- salesORDER_usd su
-- left JOIN salesperson sp
-- ON (su.salespersonid=sp.businessentityid);

insert into public.correlation_subtotal_comission_currency_ok (correlation,date)
with sales_rate_currency as (SELECT s.orderdate,s.salespersonid,s.currencyrateid, s.subtotal,c.averagerate FROM 
                     currencyrate c join
                     salesorderheader s on c.currencyrateid = s.currencyrateid),
     salesorder_currency AS (SELECT s.salespersonid,s.currencyrateid, s.subtotal,
                                (CASE
                                WHEN s.currencyrateid IS NULL THEN 1
                                ELSE s.averagerate
                                END) as averagerate
                                FROM 
                                sales_rate_currency s
                                WHERE s.salespersonid IS NOT NULL
                                AND s.orderdate BETWEEN '2014-01-01'::date 
                                 AND '2014-12-31'::date),
    salesorder_usd AS (SELECT salespersonid,subtotal*averagerate as subtotal_usd
                    FROM 
                    salesorder_currency
                    ORDER BY subtotal_usd DESC)
SELECT corr(su.subtotal_usd, sp.commissionpct) as correlation,GETDATE() as date FROM 
salesORDER_usd su
left JOIN salesperson sp
ON (su.salespersonid=sp.businessentityid);


select * from public.correlation_subtotal_comission_currency_ok;