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

CREATE TABLE #DBCCINDS
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

INSERT INTO #DBCCINDS
 EXEC (N'DBCC IND(N''trmpdb'', N''dbo.T1'', 0)');

CREATE CLUSTERED INDEX idx_cl_prevpage ON #DBCCINDS(PrevPageFID, PrevPagePID);

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
	Curlevel.PageFID, CurLevel.PagePID
FROM LinkedList AS prevLevel
  JOIN #DBCCIND AS CurlLevel
   ON CurLevel.PrevPageFID = PrevLevel.PageFID
   AND CurLevel.PrevPagePID = PrevLevel.PagePID
)
SELECT 
 CAST(PageFID AS VARCHAR(MAX)) + ':'
 + CAST(PagePID AS VARCHAR(MAX)) + ' ' AS [text()]
 FROM LinkedList
 ORDER BY RowNum
 FOR XML PATH('')
 OPTION (MAXRECURSION 0);

 DROP TABLE #DBCCINDS;


 SELECT SUBSTRING(CAST(clcol AS BINARY(16)), 11, 6) AS segment1, *
 FROM dbo.T1


 SELECT SUBSTRING(CAST(clcol AS BINARY(16)), 11, 6) AS segment1, *
 FROM dbo.T1 WITH (NOLOCK);

 SELECT SUBSTRING(CAST(clcol AS BINARY(16)), 11, 6) AS segment, *
 FROM dbo.T1 WITH (TABLOCK)

 -------------------------------------------------------------------
 SET NOCOUNT ON;
 USE tempdb;

 TRUNCATE TABLE dbo.T1;

 WHILE 1 = 1
  INSERT INTO dbo.T1 DEFAULT VALUES;

 -------------

 SET NOCOUNT ON;
 USE tempdb;

 WHILE 1 = 1
 BEGIN
  SELECT * INTO #T1 FROM dbo.T1 WITH(NOLOCK);

  IF EXISTS(
   SELECT clcol
   FROM #T1 
   GROUP BY clcol
   HAVING COUNT(*) > 1) BREAK;
   
   DROP TABLE #T1;
  END

  SELECT clcol, COUNT(*) AS cnt
  FROM #T1 
  GROUP BY clcol
  HAVING COUNT(*) > 1;

  DROP TABLE #T1;  

  ------------------------------------------------

  SET NOCOUNT ON;
  USE tempdb;

  IF OBJECT_ID(N'dbo.T1', N'U') IS NOT NULL DROP TABLE dbo.T1;

  CREATE TABLE dbo.T1
  (
	clcol UNIQUEIDENTIFIER NOT NULL DEFAULT(NEWID()),
	seqval INT NOT NULL,
	filler CHAR(2000) NOT NULL DEFAULT('a')
   );
   
  CREATE UNIQUE CLUSTERED INDEX idx_clcol ON dbo.T1(clcol);

  -- Create table MySequence
  IF OBJECT_ID(N'dbo.MySequence', N'U') IS NOT NULL DROP TABLE dbo.MySequence;

  CREATE TABLE dbo.MySequence(val INT NOT NULL);
  INSERT INTO dbo.MySequence(val) VALUES(0);

  ------

  SET NOCOUNT ON;
  USE tempdb;

  UPDATE dbo.MySequence SET val = 0;
  TRUNCATE TABLE dbo.T1;
  
  DECLARE @nextval AS INT;

  WHILE 1 = 1
  BEGIN
   UPDATE dbo.MySequence SET @nextval = val += 1
   INSERT INTO dbo.T1(seqval) VALUES(@nextval);
  END


  --------

  SET NOCOUNT ON;
  USE tempdb;

  DECLARE @max AS INT;
  WHILE 1 = 1
  BEGIN
   SET @max = (SELECT MAX(seqval) FROM dbo.T1);
   SELECT * INTO #T1 FROM dbo.T1 WITH(NOLOCK);
   CREATE NONCLUSTERED INDEX idx_seqval ON #T1(seqval);

   IF EXISTS(
	  SELECT *
	  FROM (SELECT seqval AS cur,
				(SELECT MIN(seqval)
				FROM #T1 AS N
				WHERE N.seqval > C.seqval) AS nxt
				FROM #T1 AS C
				WHERE seqval <= @max) AS D
	  WHERE nxt - cur > 1) BREAK;

	DROP TABLE #T1;
  END 

  SELECT * 
  FROM (SELECT seqval AS cur,
			(SELECT MIN(seqval)
			 FROM #T1 AS N
			 WHERE N.seqval > C.seqval) AS nxt
		FROM #T1 AS C
		WHERE seqval <= @max) AS D
  WHERE nxt - cur > 1;

  DROP TABLE #T1;

  ----------------------------------------------------

  SET NOCOUNT ON;
  USE tempdb;

  IF OBJECT_ID(N'dbo.Employees', N'U') IS NOT NULL DROP TABLE dbo.Employees;

  CREATE TABLE dbo.Employees
  (
   empid VARCHAR(10) NOT NULL,
   salary MONEY NOT NULL,
   filler CHAR(2500) NOT NULL DEFAULT('a')
   );

   CREATE CLUSTERED INDEX idx_cl_salary ON dbo.Employees(salary);
   ALTER TABLE dbo.Employees
     ADD CONSTRAINT PK_Employees PRIMARY KEY NONCLUSTERED(salary);

   INSERT INTO dbo.Employees(empid, salary) VALUES
   ('D', 1000.00),('A', 2000.00),('C', 3000.00),('B', 4000.00);
   -----------------------------------------------------------------------

   SET NOCOUNT ON;
   USE tempdb;

   WHILE 1 = 1
    UPDATE dbo.Employees
	  SET salary = 6000.00 - salary
	WHERE empid = 'D';


   SET NOCOUNT ON;
   USE tempdb;

   WHILE 1 = 1
   BEGIN 
    SELECT * INTO #Employees FROM dbo.Employees;

	IF @@ROWCOUNT < 4 BREAK; -- use < 4 for skipping, > 4 for multi occur 

	DROP TABLE #Employees;
   END

   SELECT * FROM #Employees;

   DROP TABLE #Employees;



   -----------------------------------------
   DBCC SHOW_STATISTICS (N'dbo.myTable', N'PK_Orders') WITH HISTOGRAM;

   ------------------------------------------

   DBCC OPTIMIZER_WHATIF(CPUs, 16)

   -----------------------------------------

   USE PerformanceV3;
   ALTER DATABASE PerformanceV3 SET COMPATIBILITY_LEVEL = 120;


   -----------------------------------------

   DECLARE @i AS INT = 500000;

   SELECT empid, COUNT(*) AS numorders
   FROM dbo.Orders
   WHERE orderid > @i
   GROUP BY empid
   OPTION(OPTIMIZE FOR (@i = 999900));

   -----------------------------------------

   DBCC SHOW_STATISTICS(N'dbo.Orders', N'idx_nc_cid_eid');

   ----------------------------------------------------------------------------------------

SELECT orderid, custid, empid, shipperid, orderdate, filler
FROM dbo.Orders
WHERE custid <= 'C0000001000';


SELECT orderid, custid, empid, shipperid, orderdate, filler
FROM dbo.Orders
WHERE empid <= 100;

SELECT orderid, custid, empid, shipperid, orderdate, filler
FROM dbo.Orders
WHERE custid <= 'C0000001000'
	AND empid <= 100
OPTION(QUERYTRACEON 9481);



SELECT orderid, custid, empid, shipperid, orderdate, filler
FROM dbo.Orders
WHERE custid <= 'C0000001000'
	AND empid <= 100;


SELECT orderid, custid, empid, shipperid, orderdate, filler
FROM dbo.Orders
WHERE custid <= 'C0000001000'
	OR empid <= 100;
OPTION(QUERYTRACEON 9481);

DROP INDEX idx_nc_cid_eid ON dbo.Orders;

-------------------------------------------
IF OBJECT_ID(N'dbo.Orders2', N'U') IS NOT NULL DROP TABLE dbo.Orders2;

SELECT * INTO dbo.Orders2 
FROM dbo.pokemon 
WHERE orderid <= 900000;

ALTER TABLE dbo.Orders2 ADD CONSTRAINT PK_Orders2 PRIMARY KEY NONCLUSTERED(orderid);

DBCC SHOW_STATISTICS('dbo.Orders2', 'PK_Orders2');

INSERT INTO dbo.Orders2
 SELECT *
 FROM dbo.Orders2
 WHERE orderid > 900000 AND orderid <- 1000000;


 SELECT orderid, custid, empid, shipperid, orderdate, filler
 FROM dbo.Orders2
 WHERE orderid > 900000
 ORDER BY orderdate
 OPTION(QUERYTRACEON 9481)

 -------------------------------------

 EXEC sp_helpindex N'dbo.Orders';

 idx_cl_od
 idx_nc_sid_od_cid
 idx_unc_od_oid_i_cid_eid
 PK_Orders
 
 ALTER DATABASE PerformanceV3 SET AUTO_CREATE_STATISTICS OFF;

 ------------------------------------------------------------------

 SELECT S.name AS stats_name,
    QUOTENAME(OBJECT_SCHEMA_NAME(S.object_id)) + N'.' + QUOTENAME(OBJECT_NAME(S.object_id)) AS object, 
	C.name AS column_name
 FROM sys.stats AS S
   INNER JOIN sys.stats_columns AS SC
    ON S.object_id = SC.object_id
	AND S.stats_id = SC.stats_id
   INNER JOIN sys.columns AS C
    ON SC.object_id = C.object_id
	AND SC.column_id = C.column_id 
 WHERE S.object_id = OBJECT_ID(N'dbo.Orders')
    AND auto_created = 1;


 DROPS STATISTICS dbo.Orders._WA_Sys_0000002_38EE7070;
 DROPS STATISTICS dbo.Orders._WA_Sys_0000002_38EE7070;



 -- Query 1
 DECLARE @I AS INT = 999900;

 SELECT  SELECT orderid, custid, empid, shipperid, orderdate, filler
 FROM dbo.Orders
 WHERE orderid > @i;

 -- Query 2
 SELECT orderid, custid, empid, shipperid, orderdate, filler
 FROM dbo.Orders
 WHERE custid <= 'C0000000010';

 -- Query 1
 DECLARE @i AS INT = 999901, @j AS INT = 1000000;

 SELECT orderid, custid, empid, shipperid, orderdate, filler
 FROM dbo.Orders
 WHERE orderid BETWEEN @i AND @j;

 -- Query 2
 SELECT orderid, custid, empid, shipperid, orderdate, filler
 FROM dbo.Orders
 WHERE orderid BETWEEN @i AND @j;
 OPTION(QUERYTRACEON 9481);

 -- Query 3
 SELECT orderid, custid, empid, shipperid, orderdate, filler
 FROM dbo.Orders
 WHERE custid BETWEEN 'C0000000001' AND 'C0000000010';

 -- Query 4
 SELECT orderid, custid, empid, shipperid, orderdate, filler
 FROM dbo.Orders
 WHERE custid LIKE '%9999';


  ------------
  -- Query 1
  DECLARE @i AS INT = 1000000;

  SELECT orderid, custid, empid, shipperid, orderdate, filler
  FROM dbo.Orders
  WHERE orderid = @i;

  -- Query 2
  SELECT orderid, custid, empid, shipperid, orderdate, filler
  FROM dbo.Orders
  WHERE custid = 'COOOOOOOOO1'
  
  -- Query 3
  SELECT orderid, custid, empid, shipperid, orderdate, filler
  FROM dbo.Orders
  WHERE custid = 'COOOOOOOOO1'
  OPTION(QUERYTRACEON 9481);
