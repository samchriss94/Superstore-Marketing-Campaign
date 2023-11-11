'''
=================================================
Milestone 3

Nama  : Samuel Christian S
Batch : SBY-001

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai penjualan mobil di Indonesia selama tahun 2020.
=================================================
'''


create table table_m3(
"Id" int,
"Year_Birth" int,
"Education" varchar(50),
"Marital_Status" varchar(50),
"Income" float,
"Kidhome" int,
"Teenhome" int,
"Dt_Customer" varchar(50),
"Recency" int,
"MntWines" int,
"MntFruits" int,
"MntMeatProducts" int,
"MntFishProducts" int,
"MntSweetProducts" int,
"MntGoldProds" int,
"NumDealsPurchases" int,
"NumWebPurchases" int,
"NumCatalogPurchases" int,
"NumStorePurchases" int,
"NumWebVisitsMonth" int,
"Response" int,
"Complain" int);

copy table_m3("Id", "Year_Birth", "Education", "Marital_Status", "Income", "Kidhome", "Teenhome", "Dt_Customer", "Recency", 
              "MntWines", "MntFruits", "MntMeatProducts", "MntFishProducts", "MntSweetProducts", "MntGoldProds", 
              "NumDealsPurchases", "NumWebPurchases", "NumCatalogPurchases", "NumStorePurchases", "NumWebVisitsMonth", 
              "Response", "Complain")
FROM 'C:\tmp\P2M3_samuel_christian_data_raw.csv'
DELIMITER ','
CSV HEADER;

select * from table_m3