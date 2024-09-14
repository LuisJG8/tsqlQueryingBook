SET NOCOUNT ON;
USE tempdb;

IF OBJECT_ID(N'dbo.Orders', N'U') IS NOT NULL DROP TABLE dbo.Orders;

IF OBJECT_ID(N'dbo.Customers', N'U') IS NOT NULL DROP TABLE dbo.Customers;

CREATE TABLE dbo.Customers
(
	custid CHAR(5)	NOT NULL,
	city VARCHAR(10) NOT NULL,
	CONSTRAINT PK_Customers PRIMARY KEY(custid)
);

CREATE TABLE dbo.Orders
(
	orderid INT  NOT NULL,
	custid CHAR(5) NULL,
	CONSTRAINT FK_Orders_Customers FOREIGN KEY(custid)
		REFERENCES dbo.Customers(custid)
);

GO

INSERT INTO dbo.Customers(custid, city) VALUES
	('FISSA', 'Madrid'),
	('FRNDO', 'Madrid'),
	('KRLOS', 'Madrid'),
	('MRPHS', 'Zion');

INSERT INTO dbo.Orders(orderid, custid) 
VALUES 
(1, 'FRNDO'),
(2, 'FRNDO'),
(3, 'KRLOS'),
(4, 'KRLOS'),
(5, 'KRLOS'),
(6, 'MRPHS'),
(7, NULL)

SELECT * FROM dbo.Customers;
SELECT * FROM dbo.Orders;

SELECT C.custid, COUNT(O.orderid) AS numorders
FROM dbo.Customers AS C
	LEFT OUTER JOIN dbo.Orders AS O
	ON C.custid = O.custid
WHERE C.city = 'Madrid'
GROUP BY C.custid
HAVING COUNT(O.orderid) < 3
ORDER BY numorders;