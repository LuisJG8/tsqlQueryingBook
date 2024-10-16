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

------------------------------------------------------
use master

CREATE TABLE Employees
(
  deptID INT IDENTITY(1,1) PRIMARY KEY,
  firstName NVARCHAR(50),
  lastName NVARCHAR(50),
  middleName NVARCHAR(50), 
  salary INT
 )

INSERT INTO Employees
VALUES
(
 'Pepe',
 'Mendez',
 'Jorge',
 100000
 )

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



 SELECT orderid, custid, empid, shipperid, orderdate, filler
 FROM dbo.Orders
 WHERE orderid <= 100000
 ORDER BY orderdate DESC
 OPTION(QUERYTRACEON 8649);

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

  ALTER DATABASE PerformanceV3 SET AUTO_CREATE_STATISTICS ON;

  SELECT orderid, custid, empid, shipperid, orderdate, filler
  FROM dbo.Orders
  WHERE custid LIKE '%9999';


----------------------------------------------------
SET STATISTICS TIME ON
SELECT firstName, lastName, middleName, salary, deptID

FROM dbo.Employees

WHERE salary <= 100000

ORDER BY deptId


SELECT firstName, lastName, middleName, salary, deptID

FROM dbo.Employees

WHERE salary < 1000000

ORDER BY deptId DESC



SELECT firstName, lastName, middleName, salary, deptID
FROM dbo.Employees
WHERE salary <= 100000
ORDER BY deptID DESC
OPTION(QUERYTRACEON 8649);


--Descending indexes are useful with windows functions that have a partition clause and a window order clause
SELECT deptId, salary,
	ROW_NUMBER() OVER (PARTITION BY deptID ORDER BY salary) AS rowNumber
FROM Employees;

SELECT deptId, salary,
	ROW_NUMBER() OVER (PARTITION BY deptID ORDER BY salary DESC) AS rowNumber
FROM Employees;

SELECT deptId, salary,
	ROW_NUMBER() OVER (PARTITION BY deptID ORDER BY salary DESC) AS rowNumber
FROM Employees
ORDER BY deptID DESC;

CREATE NONCLUSTERED INDEX idx_USA_orderdate
 ON Sales.Orders(orderdate)
 INCLUDE(orderidm custid, requireddate)
 WHERE shipcountry = N'USA';

 SELECT orderid, custid, orderdate, requireddate
 FROM Sales.Orders
 WHERE shipcountry = N'USA'
   AND orderdate >= '20140101'



CREATE NONCLUSTERED INDEX idx_USA_orderdate
	ON Sales.Orders(orderdate)
	INCLUDE(orderid, custid, requireddate, shipcountry)
	WHERE shipcountry = N'USA'
WITH ( DROP_EXISTING = ON );

SELECT orderid, custid, orderdate, requireddate
FROM Sales.Orders
WHERE shipcountry = N'USA'
  AND orderdate >= '20140101';


IF OBJECT_ID(N'dbo.T1', N'U') IS NOT NULL DROP TABLE dbo.T1;
CREATE TABLE dbo.T1(col1 INT NULL, col2 VARCHAR(10) NOT NULL);
GO

CREATE UNIQUE NONCLUSTERED INDEX idx_col1_notnul1 ON dbo.T1(col1) WHERE col1 IS NOT NULL;

INSERT INTO dbo.T1(col1, col2) 
VALUES (1, 'a'), (1, 'b')

INSERT INTO dbo.T1(col1, col2) 
VALUES (NULL, 'c'), (NULL, 'd');

SELECT col1, col2
FROM dbo.T1

USE tsqlv3;
DROP INDEX idx_USA_orderdate ON Sales.Orders;
DROP TABLE dbo.T1;

-------------------------------------------------
USE PerformanceV3;
SET STATISTICS IO, TIME ON

SELECT D1.attr1 AS x, D2.attr1 AS y, D3.attr1 AS Z,
 COUNT(*) AS cnt, SUM(F.measure1) AS total
FROM dbo.Fact AS F
 INNER JOIN dbo.Dim1 AS D1
   ON F.key1 = D2.key1
 INNER JOIN dbo.Dim2 AS D2
   ON F.key2 = D2.key2
 INNER JOIN dbo.Dim3 AS D3
   ON F.key3 = D3.key3
WHERE D1.attr1 <= 10
  AND D2.attr1 <= 15
  AND D3.attr1 <= 10
GROUP BY D1.attr1, D2.attr1, D3.attr1;




-----------------------------
CREATE NONCLUSTERED COLUMNSTORE INDEX idx_nc_cs
  ON dbo.Fact(key1, key2, key3, measure1, measure2, measure3, measure4);

--------------------------------------------------------------------------------------------------------------

SELECT D1.attr1 AS x, D2.attr1 AS y, D3.attr1 AS z,
 COUNT(*) AS cnt, SUM(F.measure1) AS total
FROM dbo.Fact AS F
 INNER JOIN dbo.Dim1 AS D1
  ON F.key1 = D1.key1
 INNER JOIN dbo.Dim2 AS D2
  ON F.key2 = D2.key2
 INNER JOIN dbo.Dim3 AS D3
  ON F.key3 = D3.key3
WHERE D1.attr1 <= 10
 AND D2.attr1 <= 15
 AND D3.attr1 <= 10
GROUP BY D1.attr1, D2.attr1, D3.attr1;

-------------------------------------------

CREATE TABLE dbo.FactsCS
(
 key1 INT NOT NULL,
 key2 INT NOT NULL,
 key3 INT NOT NULL,
 measure1 INT NOT NULL,
 measure2 INT NOT NULL,
 measure3 INT NOT NULL,
 measure4 NVARCHAR(50) NULL,
 filler BINARY(100) NOT NULL DEFAULT (0x)
);

CREATE CLUSTERED COLUMNSTORE INDEX idx_cl_cs ON dbo.FactsCS;

INSERT INTO dbo.FactsCS WITH (TABLOCK) 
SELECT * FROM dbo.FactsCS;

ALTER TABLE dbo.FactsCS REBUILD;

SELECT D1.attr1 AS x, D2.attr1 AS y, D3.attr1 AS z,
 COUNT (*) AS cnt, SUM(F.measure1) AS total
FROM dbo.FactsCS AS F
 INNER JOIN dbo.Dim1 AS D1
  ON F.key1 = D1.key1
 INNER JOIN dbo.Dim2 AS D2
  ON F.key2 = D3.key2
 INNER JOIN dbo. Dim3 AS D3
  ON F.key3 = D3.key3
WHERE D1.attr1 <= 10
  AND D2.attr1 <= 15
  AND D3.attr1 <= 10
GROUP BY D1.attr1, D2.attr1, D3.attr1
  
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
DROP INDEX idx_nc_cs ON dbo.Fact;
DROP TABLE dbo.FactsCS;

---------------------------

--Inline Index definition
DECLARE @T1 AS TABLE 
( 
 col1 INT NOT NULL,
  INDEX idx_cl_col1 CLUSTERED, -- column index
 col2 INT NOT NULL,
 col3 INT NOT NULL,
 INDEX idx_nc_col2_col3 NONCLUSTERED (col2, col3) -- table index
 );

 --------------------------
 --Prioritizing queries for tuning with extended events

 -- The following code query the view in the system
 SELECT * FROM sys.dm_exec_query_stats;

 SELECT TOP (5)
  MAX(query) AS sample_query,
  SUM(execution_count) AS cnt,
  SUM(total_worker_time) AS cpu,
  SUM(total_physical_reads) AS reads,
  SUM(total_logical_reads) AS logical_reads,
  SUM(total_elapsed_time) AS duration
 FROM (SELECT 
		 QS.*,
		 SUBSTRING(ST.text, (QS.statement_start_offset/2) + 1,
		   ((CASE statement_end_offset
			 WHEN -1 THEN DATALENGTH(ST.text)
			 ELSE QS.statement_end_offset END
			     - QS.statement_start_offset)/2) + 1
	  ) AS query
	FROM sys.dm_exec_query_stats AS QS
	  CROSS APPLY sys.dm_exec_sql_text(QS.sql_handle) AS ST
	  CROSS APPLY sys.dm_exec_plan_attributes(QS.plan_handle) AS PA
	WHERE PA.attribute = 'dbid'
	  AND PA.value = DB_ID('PerformanceV3')) AS D
  GROUP BY query_hash
  ORDER BY duration DESC;

  SELECT * FROM sys.dm_exec_procedure_stats;
  SELECT * FROM sys.dm_exec_trigger_stats;

  --Temporary objects
  SET STATISTICS IO, TIME ON;

  SELECT YEAR(orderdate) AS orderyear, COUNT(*) AS numorders
  FROM dbo.Customers
  GROUP BY YEAR(orderdate);


  WITH C AS 
  (
    SELECT(YEAR(orderdate) AS orderyear, COUNT(*) AS numorders
	FROM dbo.Orders
	GROUP BY YEAR(orderdate)
  )

  SELECT C1.orderyear, C1.numorders,
   A.orderyear AS otheryear, C1.numorders - A.numorders AS diff
  FROM C AS C1 CROSS APPLY
   (SELECT TOP (1) C2.orderyear, C2.numorders
    FROM C AS C2
	WHERE C2.orderyear <> C1.orderyear
	ORDER BY ABS(C1.numorders - C2.numorders)) AS A
  ORDER BY C1.orderyear;


  DECLARE @T AS TABLE
  (
   orderyear INT,
   numorders INT
  );

  INSERT INTO @T(orderyear, numorders)
   SELECT YEAR(orderdate) AS orderyear, COUNT(*) AS numorders
   FROM dbo.Orders
   GROUP BY YEAR(orderdate)
   
   SELECT T1.orderyear, T1.numorders,
    A.orderyear AS otheryear, T1.numorders - A.numorders AS diff
   FROM @T AS T1 CROSS APPLY
     (SELECT TOP (1) T2.orderyear, T2.numorders
	  FROM @T AS T2
	  WHERE T2.orderyear <> T1.orderyear
	  ORDER BY ABS(T1.numorders - T2.numorders)) AS A
   ORDER BY T1.orderyear;



   DECLARE @T AS TABLE
   (
    col1 INT NOT NULL PRIMARY KEY NONCLUSTERED,
	col2 INT NOT NULL,
	filler CHAR(200) NOT NULL
   );

   INSERT INTO @T (col1, col2, filler)
     SELECT n AS col1, n AS col2, 'a' AS filler
	 FROM TSQLV3.dbo.GetNums(1, 100000) AS Nums;

   SELECT col1, col2, filler
   FROM @T
   WHERE col1 <= 100
   ORDER BY col2;

   SELECT col1, col2, filler
   FROM @T
   WHERE col1 <= 100
   ORDER BY col2
   OPTION(RECOMPILE)

   SELECT col1, col2, filler
   FROM @T
   WHERE col1 >= 100
   ORDER BY col2
   OPTION(RECOMPILE)


	CREATE FUNCTION FnLatestHiredEmps (@CHECKDATE date, @DeptID int)
	RETURNS TABLE
	AS RETURN
	SELECT Emp.FullName, Emp.DeptID
	FROM Employees
	WHERE HireDate BETWEEN DATEADD(d, -14, @CHECKDATE) AND @CHECKDATE AND
	Emp.DeptID = @DeptID

-------------------------------------------------------
SELECT TOP(1000)
	UnitPrice
FROM Sales.SalesOrderDetail
ORDER BY
	UnitPrice DESC;


SELECT 
	th.ProductID,
	th.TransaxtionDate,
	MAX(th.ActualCost) AS MaxCost
FROM
(
    SELECT	
			tha.ProductID,
			tha.TransactionDate,
			tha.tha.ActualCost
	FROM Production.TransactionHistory AS tha
	CROSS JOIN Production.TransactionHistory AS tha
) AS th
GROUP BY 
	tha.ProductID,
	tha.TransactionDate;



SELECT 
	ActualCost
FROM Production.TransactionHistory AS th
ORDER BY 
	ActualCost DESC;


SELECT 
	p.ProductID,
	tho.TransactionID
FROM Production.Product AS p
CROSS APPLY
(
		SELECT TOP(1000)
			th.TransactionId
		FROM Production.TransactionHistory AS th
		WHERE
			th.ProductionID = p.ProductionID
		ORDER BY
			th.TransactionID
) AS thO
ORDER BY
	thO.TransactionId;


SELECT *
FROM
(
	SELECT
		sh.*,
		sd.ProductId
	FROM
	(
		SELECT TOP(1000)
			  *
		FROM Sales.SalesOrderDetail
		ORDER BY
			SalesOrderDetailId
	) AS sd
	INNER JOIN Sales.SalesOrderHeader AS sh ON
		sh.SalessOrderId = sd.SalesOrderId
) AS s;
