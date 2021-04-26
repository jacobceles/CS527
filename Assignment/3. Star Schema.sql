-- CREATING STAR SCHEMA
-- Original Table
select * from ABC_Retail_Fact_Table;

-- Employee dimension
create table employee_dim(
  id int not null auto_increment,
  first_name varchar(50),
  last_name varchar(50),
  title varchar(50),
primary key(id)
);
-- Order dimension
create table order_dim(
  id int not null,
  `date` datetime,
  shipped_date datetime,
  ship_city varchar(50),
  ship_country varchar(50),
  freight decimal(10,2),
primary key(id)
);
-- Product dimension
create table product_dim(
  id int not null auto_increment,
  name varchar(100),
primary key(id)
);
-- Customer dimension
create table customer_dim(
  id int not null auto_increment,
  name varchar(50),
  city varchar(50),
  country varchar(50),
  phone varchar(20),
  company_name varchar(200),
primary key(id)
);
-- Fact Table
create table order_fact (
  order_id int,
  product_id int,
  employee_id int,
  customer_id int,
  order_unit_price decimal (5,2),
  order_quantity int,
  order_amount decimal(10,2),
FOREIGN KEY (order_id) REFERENCES order_dim(id),
FOREIGN KEY (product_id) REFERENCES product_dim(id),
FOREIGN KEY (employee_id) REFERENCES employee_dim(id),
FOREIGN KEY (customer_id) REFERENCES customer_dim(id)
);

insert into employee_dim(first_name, last_name, title)
select distinct Employee_FirstName, Employee_LastName, Employee_Title from ABC_Retail_Fact_Table;

insert into order_dim(id, `date`, shipped_date, ship_city, ship_country, freight)
select distinct OrderID, OrderDate, Order_ShippedDate, Order_ShipCity, Order_ShipCountry, Order_Freight  from ABC_Retail_Fact_Table;

insert into product_dim(name)
select distinct ProductName from ABC_Retail_Fact_Table;

insert into customer_dim(name, city, country, phone, company_name)
select distinct Customer_ContactName, Customer_City, Customer_Country,Customer_Phone, CompanyName from ABC_Retail_Fact_Table;

insert into order_fact( order_id, product_id, employee_id, customer_id, order_unit_price, order_quantity, order_amount)
select a.OrderID, pd.id, ed.id, cd.id, a.Order_UnitPrice, a.Order_Quantity, a.Order_Amount
from ABC_Retail_Fact_Table a, order_dim od, product_dim pd, employee_dim ed, customer_dim cd
where a.OrderID=od.id
  and a.ProductName=pd.name
  and a.Employee_FirstName=ed.first_name
  and a.Employee_LastName=ed.last_name
  and a.Customer_ContactName=cd.name;
