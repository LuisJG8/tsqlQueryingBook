--SELECT TOP (1000) [custid]
--      ,[city]
--  FROM [tempdb].[dbo].[Customers]

--SELECT empid, [2013], [2014], [2015]
--INTO dbo.EmpYearValues
--FROM ( SELECT empid, YEAR(orderdate) AS orderyear, val
--	   FROM Sales.OrderValues							) AS D
--	   PIVOT (SUM(val) FOR orderyear IN([2013][2014][2015]) ) AS P;

--UPDATE dbo.EmpYearValues
--SET [2013] = NULL
--WHERE empid IN(1,2)

--SELECT empid, [2013], [2014], [2015]
--FROM dbo.EmpYearValues;



--SELECT empid, orderyear, val
--FROM dbo.EmpYearValues
--	UNPIVOT ( val FOR orderyear IN([2013],[2014],[2015]) ) AS U;


--USE TSQLV3;

--SELECT orderid, custid
--COUNT(*) OVER (PARTITION BY custid) AS numordersforcust
--FROM Sales.OrdersWHERE 
--WHERE shipcountry = N'Spain';


--SELECT orderid, custid,
--COUNT(*) OVER(PARTITION BY custid) AS numordersforcust
--FROM Sales.Orders
--WHERE shipcountry = N'Spain'
--ORDER BY COUNT(*) OVER(PARTITION BY custid) DESC;

--USE TSQLV3;

--SELECT region, city
--FROM Sales.Customers
--WHERE country = N'USA'

--INTERSECT 

--SELECT region, city
--FROM HR.Employees
--WHERE country = N'USA'

--ORDER BY region, city;

-------------------------------------------------

SET NOCOUNT ON;
USE PerfromanceV3;

SELECT orderid, custid, shipperid, orderdate, filler
FROM dbo.Orders
WHERE orderid <= 10000;

-------------------------------------------------

SET STATISTICS IO, TIME ON

CREATE EVENT SESSION query_performance ON SERVER
ADD EVENT sqlserver.sql_statement_completed(
	WHERE (sqlserver.session_id=(53)));  -- replace with your session id

ALTER EVENT SESSION query_performance ON SERVER STATE = START;

-- Use the following code to create a copy of the orders table called orders2

IF OBECT_ID(N'dbo.Orders2', N'U') IS NOT NULL DROP TABLE dbo.Orders2;
SELECT * INTO dbo.Orders2 FROM dbo.Orders;
ALTER TABLE dbo.Orders2 ADD CONSTRAINT PK_Orders2 PRIMARY KEY NONCLUSTERED (orderid);

-------------------------------------------------------------------------

SELECT orderid, custid, shipperid, orderdate, filler
FROM dbo.Orders2

-------------------------------------------------------------------------

SELECT orderid, custid, empid, orderdate
FROM dbo.Orders AS 01
WHERE orderid = 
	(SELECT MAX(orderid)
	 FROM dbo.Orders AS 02
	 WHERE 02.orderdate = 01.orderdate);

-------------------------------------------------------------------------

SET NOCOUNT ON;
USE tempdb;
GO

-- Create table T1
IF OBJECT_ID(N'dbo.T1', N'U') IS NOT NULL DROP TABLE dbo.T1;

CREATE TABLE dbo.T1
(
 clcol UNIQUEIDENTIFIER NOT NULL DEFAULT(NEWID()),
 filler CHAR(2000) NOT NULL DEFAULT('a')
);
GO
CREATE UNIQUE CLUSTERED INDEX idx_clcol ON dbo.T1(clcol);


SET NOCOUNT ON;
USE tempdb;

TRUNCATE TABLE dbo.T1;

WHILE 1 = 1
  INSERT INTO dbo.T1 DEFAULT VALUES;

SELECT avg_fragmentation_in_percent
FROM sys.dm_db_index_physical_stats
(
 DB_ID(N'tempdb'),
 OBJECT_ID(N'dbo.T1'),
 1,
 NULL,
 NULL
);

-----

--DBCC IND(N'tempdb', N'dbo.T1', 0)

CREATE TABLE #DBCCIND
(
PageFID INT,
PagePID INT,
IAMFID INT,
IAMPID INT,
ObjectiveID INT,
IndexID INT,
PartitionNumber INT,
PartitionID BIGINT,
iam_chain_type VARCHAR(100),
PageType INT,
IndexLevel INT,
NextPageFID INT,
NextPagePID INT,
PrevPageFID INT,
PrevPagePID INT
);

INSERT INTO #DBCCIND
 EXEC (N'DBCC IND(N''trmpdb'', N''dbo.T1'', 0)');

CREATE CLUSTERED INDEX idx_cl_prevpage ON #DBCCIND(PrevPageFID, PrevPagePID);

WITH LinkedList
AS
(
 SELECT 1 AS RowNum, PageFID, PagePID
 FROM #DBCCIND
 WHERE IndexID = 1
  AND IndexLevel = 0
  AND PrevPageFID = 0
  AND PrevPagePID = 0

UNION ALL

SELECT PrevLevel.RowNum + 1,
	Curlevel
