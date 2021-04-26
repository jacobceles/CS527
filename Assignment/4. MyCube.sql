-- Creating MyCube table from the ABC_Retail_Fact_Table
create table MyCube as
select
    year(OrderDate) as ThisYear
    ,concat('Q', QUARTER(OrderDate)) as ThisQuarter
    ,Order_ShipCountry as Region
    ,ProductName as Product
    ,Order_Amount as Sales
from
    ABC_Retail_Fact_Table
where
    Order_ShipCountry in ('USA','Canada','UK')
    and ProductName in ('Chai','Tofu','Chocolade');
