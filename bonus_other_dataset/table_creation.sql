
create or replace table "ADVENTUREWORKS"."PUBLIC"."COUNTRYREGIONCURRENCY"(
    countryregioncode text,
    currencycode text,
    modifiedtimestamp timestamp
);


create or replace table "ADVENTUREWORKS"."PUBLIC"."CURRENCYRATE"(
    currencyrateid  numeric,
    currencyratetimestamp timestamp,
    fromcurrencycode text,
    tocurrencycode text,
    averagerate float,
    endofdayrate float,
    modifiedtimestamp timestamp
);

create or replace table "ADVENTUREWORKS"."PUBLIC"."PRODUCT"(
    productid text primary key,
    name text,
    productnumber text,
    makeflag text,
    finishedgoodsflag text,
    color text,
    safetystocklevel numeric,
    reorderponumeric numeric,
    standardcost float,
    listprice float,
    size text,
    sizeunitmeasurecode text,
    weightunitmeasurecode text,
    weight float,
    daystomanufacture numeric,
    productline text,
    class text,
    style text,
    productsubcategoryid numeric,
    productmodelid numeric,
    sellstarttimestamp timestamp,
    sellendtimestamp timestamp,
    discontinuedtimestamp timestamp,
    rowguid text,
    modifiedtimestamp timestamp
);

create or replace table "ADVENTUREWORKS"."PUBLIC"."PRODUCTCATEGORY"(
    productcategoryid numeric primary key,
    name text,
    rowguid text,
    modifiedtimestamp timestamp
);
create or replace table "ADVENTUREWORKS"."PUBLIC"."PRODUCTDESCRIPTION"(
    productdescriptionid numeric primary key,
    description text,
    rowguid text,
    modifiedtimestamp timestamp
);
create or replace table "ADVENTUREWORKS"."PUBLIC"."PRODUCTMODELPRODUCTDESCRIPTIONCULTURE"(
    productmodelid numeric,
    productdescriptionid numeric,
    cultureid text,
    modifiedtimestamp timestamp
);

create or replace table "ADVENTUREWORKS"."PUBLIC"."PRODUCTSUBCATEGORY"(
    productsubcategoryid numeric primary key,
    productcategoryid numeric,
    name text,
    rowguid text,
    modifiedtimestamp timestamp
);
create or replace table "ADVENTUREWORKS"."PUBLIC"."SALESORDERDETAIL"(
    salesorderid numeric,
    salesorderdetailid numeric,
    carriertrackingnumber text,
    orderqty numeric,
    productid numeric,
    specialofferid numeric,
    unitprice float,
    unitpricediscount float,
    rowguid text,
    modifiedtimestamp timestamp
);
create or replace table "ADVENTUREWORKS"."PUBLIC"."SALESORDERHEADER"(
    salesorderid numeric primary key,
    revisionnumber numeric,
    orderdate date,
    duedate timestamp,
    shipdate date,
    status numeric,
    onlineorderflag  text,
    purchaseordernumber text,
    accountnumber text,
    customerid numeric,
    salespersonid numeric,
    territoryid numeric,
    billtoaddressid numeric,
    shiptoaddressid  numeric,
    shipmethodid  numeric,
    creditcardid  numeric,
    creditcardapprovalcode text,
    currencyrateid numeric,
    subtotal  float,
    taxamt float,
    freight  float,
    totaldue float,
    comment text,
    rowguid text,
    modifiedtimestamp timestamp
);
create or replace table "ADVENTUREWORKS"."PUBLIC"."SALESPERSON"(
    businessentityid numeric,
    territoryid numeric,
    salesquota numeric,
    bonus numeric,
    commissionpct float,
    salesytd float,
    saleslastyear float,
    rowguid text,
    modifiedtimestamp timestamp
);
create or replace table "ADVENTUREWORKS"."PUBLIC"."SALESTERRITORY"(
    territoryid numeric primary key,
    name text,
    countryregioncode text,
    group0 text,
    salesytd float,
    saleslastyear float,
    costytd  numeric,
    costlastyear  numeric,
    rowguid text,
    modifiedtimestamp timestamp
);