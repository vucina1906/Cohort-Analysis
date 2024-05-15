--Cleaning data

--Total records = 541909
SELECT * from online_retail

--We now examine our data. We seee that in column CustomerID we are missing some values and we we will delete these rows
--Regarding column Quantity there are some negative values. That means that customers returned some products
--135080 Records have no customerID
select * from online_retail
where CustomerID = 0

--We delete this records
--Now 406829 Records have customerID
with #online_retail as 
(
	select * from online_retail
	where CustomerID != 0
)
--We are also removing all the quantity where quantity is less than zero and unit_price less than zero
--We are getting 397882 Records with quantity and unit price
select * from #online_retail
where quantity > 0 and UnitPrice > 0

--We will pass new data in new CTE called quantity_unit_price
with #online_retail as 
(
	select * from online_retail
	where CustomerID != 0
),
quantity_unit_price as 
(
	select * from #online_retail
	where quantity > 0 and UnitPrice > 0
), dupl_checks as
(
--We are also checking duplicate values
--Everywhere where we get in column duplicate_flag greater than 1 that means dupicates exist and we should not use these rows
	select *,ROW_NUMBER() over(partition by InvoiceNo,StockCode,Quantity order by InvoiceDate) as duplicate_flag --so check all the rows with same InvocieNo,StockCode and Quantity and order them by InoviceDate to check duplicates
	from quantity_unit_price
)
--Now without duplicates we have 392667 cleaned data that we will hold into temp table for further analysis
select * into cleaned_data
from dupl_checks
where duplicate_flag = 1

--Now we do analysis from temp table called cleaned_data
select * from cleaned_data

--Analysis
--To create cohorts we need unique identifier for the groups that we are going to analyse
--Unique Identifier (CustomerID)
--We also need initial start date and since we are ding retention analysis we are going to use first invoicedate  asthe initial start date and this will help us come up with a cohort group
--And we need revenue data 

--So we check for each customer first date , using InoviceDate and put results in another temp table
select CustomerID,
	min(invoiceDate) as first_purchase_date,
	DATEFROMPARTS(year(min(invoiceDate)),month(min(invoiceDate)),1) Cohort_date --We will separate our cohort groups on months and year basis so we will extract month and year from first purchase date, 1 represent the day that is not important so it will be 1 as default
into #cohort
from cleaned_data
group by CustomerID

--Now we can continue analysis on #cohort temporary table 
--Create cohort index (first we find months and year from inovicedate and cohort date and from difference we will create index)
--So when we get index 1 that means that the customer made their second purchase  the same month they made their first one
--We will store our final results into table that we will use in Tableau
select
	mmm.*, 
	cohort_index = year_diff * 12 + month_diff + 1 --so we get cohort index or number of months that pased since customer first made a purchase
into #cohort_retention
from
(
	select 
		mm.*, 
		year_diff = invoice_year - cohort_year,
		month_diff = invoice_month - cohort_month
	from 
	(
		select 
			cd.*, 
			coh.Cohort_date,
			year(InvoiceDate) invoice_year,
			month(InvoiceDate) invoice_month,
			year(Cohort_date) cohort_year,
			month(Cohort_date) cohort_month

		from cleaned_data as cd
		left join #cohort as coh
		on cd.CustomerID = coh.CustomerID
	) mm
) mmm

--Now everything is in #cohort_retention
--Extract temporary table into csv file using result result as option in SSMS

--Now find unique customers and their cohort_date and cohort_index
--Down bellow we can see that for example customer 12347 made their first purchase in first month than second month than in fifth month 
select distinct customerid, cohort_date, cohort_index 
from #cohort_retention
order by 1,3

--Now we will check how many customers returned in a cohort month
--Pivot data to see the cohort table. For pivot table we need to use aggregate functions. We will use customerid for index 
select *
into #cohort_pivot_table
from
(
	select distinct customerid, cohort_date, cohort_index 
	from #cohort_retention
) tbl
pivot(
	count(CustomerID) for Cohort_Index in 
	([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13]) -- we can check how manz cohort indexes we have using "select distinct cohort index from #cohort_retention"

) as pivot_table
--So we given the december 2010 the total number showeed up in that month is 895. Out of that 895 324 returned in the next month and so on..

--Now we will convert previous result in percentages so we deal each column / [1] becuase in index [1] are all customers that appeared than we check how much we retain in next months

select *,
	1.0 * [1]/[1] * 100 as [1],
	1.0 * [2]/[1] * 100 as [2],
	1.0 * [3]/[1] * 100 as [3],
	1.0 * [4]/[1] * 100 as [4],
	1.0 * [5]/[1] * 100 as [5],
	1.0 * [6]/[1] * 100 as [6],
	1.0 * [7]/[1] * 100 as [7],
	1.0 * [8]/[1] * 100 as [8],
	1.0 * [9]/[1] * 100 as [9],
	1.0 * [10]/[1] * 100 as [10],
	1.0 * [11]/[1] * 100 as [11],
	1.0 * [12]/[1] * 100 as [12],
	1.0 * [13]/[1] * 100 as [13]
from #cohort_pivot_table
order by Cohort_date