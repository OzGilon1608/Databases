CREATE TABLE Customers (
Email Varchar (50)  not null,
FirstName Varchar (10) not null,
LastName Varchar (10) not null,
Phone Varchar (30) not null,
Address Varchar(30) not null,
Country Varchar(30) not null,
 Primary Key (Email),
)


CREATE TABLE CreditCards (
NumCard Varchar(50)  not null ,
Types Varchar(30) not null,
ExpirationMonth TinyInt not null,
ExpirationYear Integer not null,
CVV Varchar(3) not null,
Email Varchar(50) not null ,
Primary Key (NumCard),
CONSTRAINT fk_Email FOREIGN KEY (Email) REFERENCES Customers(Email),
)


CREATE TABLE Searches(
IPAddress Varchar(30)  not null,
SearchDT DateTime  not null,
ActivityName Varchar(30) not null,
Email Varchar(50)  null,
Primary Key (IPAddress, SearchDT),
CONSTRAINT fk_EmailS FOREIGN KEY (Email) REFERENCES Customers(Email),
)


CREATE TABLE Purchases(
PurchaseID Varchar(30) not null,
Country Varchar(30) not null,
Price SmallMoney not null,
Primary Key (PurchaseID),
)


CREATE TABLE Attractions (
PurchaseID  Varchar(30) not null,
CustomerRate TinyInt not null,
PurchaseDT DateTime  null,
ActivityName Varchar(50) not null,
Primary Key (PurchaseID),
CONSTRAINT fk_PurchaseIDA FOREIGN KEY (PurchaseID) REFERENCES Purchases (PurchaseID),
)


CREATE TABLE Hotels(
PurchaseID  Varchar(30) not null,
Street Varchar(50) not null,
HotelName Varchar(50) not null,
RoomType Varchar(20) not null,
Primary Key (PurchaseID),
CONSTRAINT fk_PurchaseIDH FOREIGN KEY (PurchaseID) REFERENCES Purchases (PurchaseID),

)

CREATE TABLE Facilities (
PurchaseID  Varchar(30)  not null,
Facilities Varchar(30)  not null,
Primary Key ( PurchaseID, Facilities),
CONSTRAINT fk_PurchaseIDF  FOREIGN KEY (PurchaseID) REFERENCES Hotels(PurchaseID),
)

CREATE TABLE Bundles (
Bundle Varchar(30)  not null,
BundleName Varchar(50)  not null,
Primary Key(Bundle, BundleName)
)


CREATE TABLE Orders(
OrdersID Integer  not null,
Email Varchar(50) not null,
NumCard Varchar(50) not null,
OrderDT DateTime  not null,
IPAddress Varchar(30) not null,
SearchDT DateTime  not null,
Primary Key (OrdersID),
CONSTRAINT fk_NumCardsO  FOREIGN KEY (NumCard) REFERENCES CreditCards(NumCard),
CONSTRAINT fk_OrdersO FOREIGN KEY (IPAddress, SearchDT) REFERENCES Searches(IPAddress, SearchDT),
)

CREATE TABLE Includes(
OrdersID Integer  not null,
PurchaseID  Varchar(30) not null,
Primary Key (OrdersID,PurchaseID),
CONSTRAINT fk_OrdersIDI  FOREIGN KEY (OrdersID) REFERENCES Orders(OrdersID),
CONSTRAINT fk_PurchaseIDI  FOREIGN KEY (PurchaseID) REFERENCES Purchases (PurchaseID),
)

CREATE TABLE Retrieves(
IPAddress Varchar(30)not null,
SearchDT DateTime  not null,
PurchaseID  Varchar(30) not null,
Primary Key (IPAddress,PurchaseID,SearchDT),
CONSTRAINT fk_OrdersR FOREIGN KEY (IPAddress,SearchDT) REFERENCES Searches(IPAddress, SearchDT),
CONSTRAINT fk_PurchaseIDR  FOREIGN KEY (PurchaseID) REFERENCES Purchases(PurchaseID),
)
------------------------
DROP TABLE Retrieves
DROP TABLE Includes
DROP TABLE Orders
DROP TABLE Bundles
DROP TABLE Facilities
DROP TABLE Hotels
DROP TABLE Attractions
DROP TABLE Purchases
DROP TABLE Searches
DROP TABLE CreditCards
DROP TABLE Customers

------------------------------------------------


	ALTER TABLE Customers ADD CONSTRAINT CK_EMAIL CHECK (Email LIKE '%@%.%')
	
	ALTER TABLE Purchases ADD CONSTRAINT CK_Price Check (Price>0)

	ALTER TABLE Attractions ADD CONSTRAINT CK_CustomerRate Check (CustomerRate>0 AND CustomerRate<11)

	ALTER TABLE CreditCards ADD CONSTRAINT CK_Expiration_month Check (Expiration_month >0 AND Expiration_month <13)

	ALTER TABLE CreditCards ADD CONSTRAINT CK_Expiration_Year Check (Expiration_Year>Year(getdate()))

	----------------------------------------------------------------------------------------

	CREATE TABLE CreditCardType (Types VarChar (30) Primary Key NOT NULL )
	INSERT INTO CreditCardType VALUES ('VISA'),('MASTERCARD'),('AMERICAN EXPRESS'),('DIRECT')	
	ALTER TABLE CreditCards ADD CONSTRAINT FK_CreditCardType FOREIGN KEY (Types) REFERENCES CreditCardType (Types)

	INSERT INTO Hotels(RoomType)
	VALUES ('STANDARD'),('GARDEN VIEW'),('SUITE'),('PREMIUM')

	   
---------------------------------------------------------------------------------------- 1
SELECT 		C.Email, C.Country, [Full Name]= C.FirstName + ' ' + C.LastName ,  [Number Of Orders] = count(	*)
FROM		CUSTOMERS AS C JOIN CREDITCARDS AS CC ON C.Email=CC.EMAIL 
	        JOIN ORDERS AS O ON O.NUMCARD = CC.NUMCARD 
WHERE		year(O.OrderDT) = 2022 AND CC.Types LIKE 'mastercard'
GROUP BY	C.Email, C.COUNTRY,  C.FirstName + ' ' + C.LastName 
HAVING		count(*) >=2
ORDER BY	[Number Of Orders] DESC

---------------------------------------------------------------------------------------- 2
SELECT 		C.Email, C.Country , [Full Name]= C.FirstName + ' ' + C.LastName ,  O.OrderDT 
FROM		SEARCHES AS S JOIN CUSTOMERS AS C ON S.EMAIL=C.EMAIL 
			JOIN ORDERS AS O ON O.IPAddress = S.IPAddress AND O.SearchDT = S.SearchDT
WHERE		S.SEARCHDT = O.ORDERDT AND S.ActivityName LIKE 'Walt Disney'
GROUP BY	C.Email, C.COUNTRY,  C.FirstName + ' ' + C.LastName ,  O.ORDERDT
ORDER BY	O.ORDERDT DESC

--------------------------------------------------------- 3
SELECT    [Month Order]= MONTH(O.OrderDT), [Ratio Price]=SUM (P.PRICE)/    (
          SELECT TOTALPRICE=SUM (P.PRICE) 
		  FROM Includes AS I JOIN orders AS O ON I.OrdersID= O.OrdersID
	      JOIN Purchases AS P ON I.PurchaseID= P.PurchaseID
		  WHERE YEAR(O.OrderDT)= '2020'
		  )		
FROM  Includes AS I JOIN orders AS O ON I.OrdersID= O.OrdersID
	   JOIN Purchases AS P ON I.PurchaseID= P.PurchaseID
WHERE	MONTH(O.OrderDT) BETWEEN 1 AND 12 AND YEAR(O.OrderDT)= '2020'
GROUP BY MONTH(O.OrderDT)
ORDER BY MONTH(O.OrderDT)

--------------------------------------------------------- 4

SELECT 		S.ActivityName
FROM		Searches AS S JOIN CUSTOMERS AS C ON S.EMAIL=C.EMAIL 
WHERE		C.COUNTRY='Brazil' AND S.ActivityName NOT IN (
              SELECT DISTINCT S.ActivityName
			  FROM Searches AS S JOIN CUSTOMERS AS C ON S.EMAIL=C.EMAIL
			  WHERE C.COUNTRY= 'Poland'
			  )
GROUP BY	S.ActivityName

--------------------------------------------------------- 5

--ALTER TABLE CUSTOMERS DROP COLUMN [Active Customer]
ALTER TABLE CUSTOMERS ADD [Active Customer] bit
 
UPDATE CUSTOMERS SET [Active Customer] =1
		WHERE CUSTOMERS.Email IN (
						SELECT O.Email 
						FROM ORDERS AS O 
						WHERE YEAR(getdate())= YEAR(O.OrderDT)
										)

UPDATE CUSTOMERS SET [Active Customer] =0
		WHERE CUSTOMERS.Email NOT IN (
						SELECT O.Email
						FROM ORDERS AS O 
						WHERE YEAR(getdate())= YEAR(O.OrderDT)
										)

SELECT *
FROM CUSTOMERS AS C
ORDER BY [Active Customer] DESC

--------------------------------------------------------- 6

	SELECT  O.Email, P.Price
	FROM Hotels AS H JOIN Purchases AS P ON H.PurchaseID= P.PurchaseID 
		JOIN Includes AS I ON P.PurchaseID = I.PurchaseID
		JOIN Orders AS O ON O.OrdersID=I.OrdersID
	WHERE YEAR(getdate())= YEAR(O.OrderDT) AND P.PRICE > (
                       SELECT  MAX(P.PRICE) 
						FROM Includes AS I JOIN ORDERS AS O ON I.OrdersID= O.OrdersID
							 JOIN Purchases AS P ON I.PurchaseID= P.PurchaseID
						WHERE  YEAR(O.OrderDT) = YEAR(getdate())-1
					)
		UNION

	SELECT  O.Email,  P.Price
	FROM Attractions AS A JOIN Purchases AS P ON A.PurchaseID= P.PurchaseID 
		JOIN Includes AS I ON P.PurchaseID = I.PurchaseID
		JOIN Orders AS O ON O.OrdersID=I.OrdersID
	WHERE YEAR(getdate())= YEAR(O.OrderDT) AND P.PRICE > (
	                   SELECT  MAX(P.PRICE) 
						FROM Includes AS I JOIN orders AS O ON I.OrdersID=   O.OrdersID JOIN Purchases AS P ON I.PurchaseID= P.PurchaseID
						WHERE  YEAR(O.OrderDT) = YEAR(getdate())-1
					)

						 
--------------------------------------------------------- מטלה 2


--DROP VIEW VIEW_ORDERS
CREATE VIEW VIEW_ORDERS
AS
SELECT			C.Email, [Full Name] = C.FirstName+ ' '+C.LastName, C.Address, [Total Price] = SUM(P.PRICE)	
FROM		SEARCHES AS S JOIN CUSTOMERS AS C ON S.EMAIL=C.EMAIL 
			JOIN ORDERS AS O ON O.IPAddress = S.IPAddress AND O.SearchDT = S.SearchDT JOIN
			Includes AS I ON I.OrdersID= O.OrdersID
			JOIN Purchases AS P ON I.PurchaseID= P.PurchaseID
WHERE			C.Email=O.Email 
GROUP BY	C.Email,  C.FirstName+' '+C.LastName, C.Address	

SELECT TOP 10 *
FROM VIEW_ORDERS
ORDER BY [Total Price] DESC
--------------------------------------------------------- פונקצייה ראשונה
--DROP VIEW VIEW_ATTRACTIONS
CREATE VIEW VIEW_ATTRACTIONS
AS
SELECT		A.ActivityName, O.OrdersID	, O.OrderDT
FROM	Attractions AS A JOIN Purchases AS P ON A.PurchaseID= P.PurchaseID 
		JOIN Includes AS I ON P.PurchaseID = I.PurchaseID
		JOIN Orders AS O ON O.OrdersID=I.OrdersID
 WHERE O.OrdersID=I.OrdersID and I.PurchaseID=P.PurchaseID

 ---------------------------------------------------------
	
--DROP FUNCTION  NUM_OF_PURCHASES
CREATE 	FUNCTION  NUM_OF_PURCHASES ( @ATTRACTIONNAME VARCHAR (50), @From Datetime, @To Datetime ) 
RETURNS	int
AS 	BEGIN
		DECLARE 	@Total	Int
		SELECT    @Total = COUNT (V.OrdersID) 
		FROM	VIEW_ATTRACTIONS AS V
		WHERE  	v.ActivityName LIKE @ATTRACTIONNAME and YEAR(V.OrderDT) >= @From AND
				YEAR(V.OrderDT) <= @To

		RETURN 	@Total
		END

		SELECT [Amount Of Orders] = dbo.NUM_OF_PURCHASES('WonderWorks', 2016,2022)
--------------------------------------------------- פונקציה שנייה

		--DROP VIEW View_Customer
		CREATE VIEW View_Customer
		AS
		SELECT	C.Email, O.OrdersID,O.OrderDT,A.ActivityName,A.CustomerRate, H.HotelName, P.PRICE 
		FROM	SEARCHES AS S JOIN CUSTOMERS AS C ON S.EMAIL=C.EMAIL 
			JOIN ORDERS AS O ON O.IPAddress = S.IPAddress AND O.SearchDT = S.SearchDT JOIN
			 Includes AS I ON I.OrdersID= O.OrdersID
	       JOIN Purchases AS P ON I.PurchaseID= P.PurchaseID
		   JOIN Attractions AS A ON A.PurchaseID= P.PurchaseID
		   JOIN Hotels AS H ON H.PurchaseID= p.PurchaseID
		 WHERE O.OrderSID=I.OrdersID and I.PurchaseID=P.PurchaseID

		-- DROP FUNCTION Customer_Report
		CREATE FUNCTION Customer_Report (@CustomerEmail Varchar(50))
		RETURNS TABLE
		AS RETURN
		SELECT  V.OrdersID,V.OrderDT,V.ActivityName,V.CustomerRate, V.HotelName, V.Price
		FROM  View_Customer AS V
		WHERE  V.Email = @CUSTOMEREmail
		GROUP BY V.OrdersID,V.OrderDT,V.ActivityName,V.CustomerRate, V.HotelName, V.Price 


		SELECT *
		FROM dbo.Customer_Report('efillis3w@furl.net')
		ORDER BY OrderDT

----------------------------------------------------- TRIGGER

--ALTER TABLE CUSTOMERS DROP column TotalPurchases;
ALTER TABLE CUSTOMERS ADD  TotalPurchases int

--DROP TRIGGER Update_Orders
CREATE TRIGGER Update_Orders
ON	ORDERS  
FOR INSERT, UPDATE, DELETE
AS	BEGIN
	UPDATE	Customers SET totalPurchases = (
			SELECT COUNT(*)
			FROM Orders
			WHERE Customers.Email=Orders.Email
			)
	WHERE Email IN (
		SELECT DISTINCT Email FROM INSERTED
		UNION 
		SELECT DISTINCT Email FROM DELETED
	)
	END

INSERT INTO Searches VALUES	('3.195.208.163', '2020-08-22 00:00:00.000', 'Meal Tour', 'efillis3w@furl.net')
INSERT INTO Orders VALUES ('60823', 'efillis3w@furl.net', '1795966839332760',	'2020-08-22 00:00:00.000',	'3.195.208.163',	'2020-08-22 00:00:00.000') 

SELECT *
FROM Customers AS C
WHERE C.Email = 'efillis3w@furl.net'

----------------------------------------------------- SP

--DROP VIEW View_CustomerRate
CREATE VIEW View_CustomerRate
AS
SELECT	C.Email ,A.CustomerRate
FROM	SEARCHES AS S JOIN CUSTOMERS AS C ON S.EMAIL=C.EMAIL 
			JOIN Orders AS O ON O.IPAddress = S.IPAddress AND O.SearchDT = S.SearchDT 
			JOIN Includes AS I ON I.OrdersID= O.OrdersID
	       JOIN Purchases AS P ON I.PurchaseID= P.PurchaseID
		   JOIN Attractions AS A ON A.PurchaseID= P.PurchaseID		
WHERE O.OrderSID=I.OrdersID and I.PurchaseID=P.PurchaseID


--DROP PROCEDURE SP_Rank_CustomerRate
CREATE  PROCEDURE SP_Rank_CustomerRate
@CustomerEmail Varchar(50), @newRate tinyInt , @PurchaseID int
AS	BEGIN
	UPDATE Attractions SET CustomerRate = @newRate
	WHERE 	PurchaseID = @PurchaseID 
	END
	RETURN

	EXECUTE SP_Rank_CustomerRate 'abannister3y@hhs.gov'  ,'3', '10702'
	
	SELECT *
	FROM View_CustomerRate AS V

 AND O.SearchDT = S.SearchDT

------------------------------------------------------דוח עסקי מטלה 3
	--DROP VIEW VIEW_Num_Of_Orders
	CREATE VIEW VIEW_Num_Of_Orders
	AS
	SELECT P.Country, [Number Of Orders]=COUNT(*)
	FROM      Purchases AS P
	JOIN Includes AS I ON P.PurchaseID = I.PurchaseID
	JOIN Orders AS O ON O.OrdersID=I.OrdersID
	WHERE YEAR(O.OrderDT)=YEAR(GETDATE())
	GROUP BY P.Country

	-----------------------------------------------------
	--DROP VIEW VIEW_AttractionRate
	CREATE VIEW VIEW_AttractionRate
	AS
	SELECT  A.ActivityName, [Average Rate] = AVG(A.CustomerRate) 
	FROM    Includes AS I JOIN Orders AS O ON O.OrdersID=I.OrdersID JOIN Purchases AS P
	ON P.PurchaseID = I.PurchaseID JOIN Attractions AS A ON A.PurchaseID=P.PurchaseID
	WHERE YEAR(O.OrderDT)=YEAR(GETDATE())
	GROUP BY A.ActivityName

	----------------------
	--DROP VIEW VIEW_Purchase_Price
	CREATE VIEW VIEW_Purchase_Price
	AS 
	SELECT		Month=month(O.OrderDT), [Revenue by Month] = SUM(p.price)
	FROM		Includes AS I JOIN Orders AS O ON O.OrdersID=I.OrdersID JOIN Purchases AS P
				ON P.PurchaseID = I.PurchaseID JOIN Attractions AS A ON A.PurchaseID=P.PurchaseID
	WHERE		YEAR(O.OrderDT)=YEAR(GETDATE())
	GROUP BY	MONTH(O.OrderDT),YEAR(O.OrderDT)

------------------------------------------------------------------
	--DROP VIEW Number_Of_Bundle_Orders
	CREATE VIEW Number_Of_Bundle_Orders
	AS
	SELECT	Bundle = B.BundleName, [Number of Orders] = COUNT (*)
	FROM	Purchases AS P
			JOIN Attractions AS A ON A.PurchaseID= P.PurchaseID
			JOIN Bundles as B on B.BundleName= a.ActivityName
			JOIN Includes as I on I.PurchaseID = P.PurchaseID
			JOIN Orders as O on O.OrdersId = I.OrdersID
	GROUP BY B.BundleName

	------------------ לוח מחוונים 
	--DROP VIEW VIEW_REPORT
	CREATE VIEW VIEW_REPORT
	AS
	SELECT	CC.Email, CC.FirstName, CC.LastName, CC.Country, S.IPAddress, S.SearchDT, [Search by Activity Name] = S.ActivityName, C.Types, O.OrdersID, O.OrderDT, P.PurchaseID, [Country of Activity] = P.Country, P.Price, [Purchase by Activity Name] = A.ActivityName, A.CustomerRate, H.HotelName, H.RoomType
	FROM		ORDERS AS O JOIN CreditCards AS C ON  O.NUMCARD = C.NUMCARD  join Customers as CC on CC.Email=o.Email
			    JOIN SEARCHES AS S ON O.IPAddress = S.IPAddress AND O.SearchDT = S.SearchDT
				JOIN Retrieves AS R ON R.IPAddress = S.IPAddress AND R.SearchDT = S.SearchDT
				JOIN Purchases AS P ON R.PurchaseID= P.PurchaseID
				JOIN Attractions AS A ON A.PurchaseID= P.PurchaseID		
				JOIN Hotels AS H ON H.PurchaseID= p.PurchaseID


------------------------------------------------------------------------------------------------------------------------------------------------------------------

	--DROP VIEW V_ TargetForOrder 
	 CREATE VIEW V_TargetForOrder
	AS
	SELECT [Target]=  ROUND (1.15 * COUNT(o.OrdersID),0)
	FROM  Orders as O
	WHERE year(o.OrderDT)=YEAR(GETDATE()-1)

	--DROP VIEW V_sumOfOrder 
	CREATE VIEW V_sumOfOrder	
	AS
	SELECT  COUNT(o.OrdersID) AS sum1
	FROM  Orders as O
	WHERE YEAR(o.OrderDT)=YEAR(GETDATE()-1)

	--DROP VIEW V_TargetForRevenue
	CREATE VIEW V_TargetForRevenue
	AS
	SELECT [Target] = 1.05 * SUM(P.Price)
	FROM   Includes AS I JOIN Orders AS O ON O.OrdersID=I.OrdersID JOIN Purchases AS P
			ON P.PurchaseID = I.PurchaseID
	WHERE  YEAR (o.OrderDT)= YEAR(GETDATE()-1)

 
	--DROP VIEW V_sumRevenue  
	CREATE VIEW V_sumRevenue 
	AS
	SELECT  [Current Year] = SUM(P.Price)
	FROM	Includes AS I JOIN Orders AS O ON O.OrdersID=I.OrdersID JOIN Purchases AS P
			ON P.PurchaseID = I.PurchaseID
	WHERE	YEAR(o.OrderDT)=YEAR(GETDATE())
 
	
 

	 ---------------------------------------------------------------- מטלה 4
	
	SELECT V.Year, V.[Sum Per Year], [Growth By Percentage] = (V.[Sum Per Year] / LAG (V.[Sum Per Year]) OVER (ORDER BY YEAR)) -1
	FROM (
			SELECT		[Year] = YEAR(O.OrderDT), [Sum Per Year] = SUM (P.Price)
			FROM		Includes AS I JOIN Orders AS O ON O.OrdersID =I.OrdersID JOIN Purchases AS P
						ON P.PurchaseID = I.PurchaseID
			GROUP BY	YEAR(O.OrderDT)
		  ) AS V
	
	--------------------------------------------------------------------------------


	SELECT DISTINCT  X.[Full Name], X.Country, V.Email, V.[Sum Per Customer],
			[Percent By Country] = PERCENT_RANK () OVER (PARTITION BY X.Country ORDER BY V.[Sum Per Customer]),
			[Cume Dist] = CUME_DIST() OVER (PARTITION BY X.Country ORDER BY V.[Sum Per Customer]) ,
			[Rank] = DENSE_RANK() OVER (PARTITION BY X.Country ORDER BY V.[Sum Per Customer]  )
	FROM		( 
			SELECT [Full Name] = C.FirstName + ' ' + C.LastName, C.Country , C.Email
			FROM	SEARCHES AS S JOIN CUSTOMERS AS C ON S.EMAIL=C.EMAIL 
					JOIN ORDERS AS O ON O.IPAddress = S.IPAddress AND O.SearchDT = S.SearchDT JOIN
					Includes AS I ON I.OrdersID= O.OrdersID
					JOIN Purchases AS P ON I.PurchaseID= P.PurchaseID
				) AS X JOIN
				(
				SELECT	C.Email, [Sum Per Customer] = SUM(P.Price) 
							FROM	SEARCHES AS S JOIN CUSTOMERS AS C ON S.EMAIL=C.EMAIL 
									JOIN ORDERS AS O ON O.IPAddress = S.IPAddress AND O.SearchDT = S.SearchDT JOIN
									Includes AS I ON I.OrdersID= O.OrdersID
									JOIN Purchases AS P ON I.PurchaseID= P.PurchaseID
							GROUP BY C.Email
				) AS V ON X.Email = V.Email
			ORDER BY X.Country, V.[Sum Per Customer] DESC
			
			

--------------Procedure
--drop procedure sp_Rate
create procedure sp_Rate(@fromDate datetime, @toDate datetime)
as
	if (select OBJECT_ID ('attractionRate')) is not null drop table attractionRate
	create table attractionRate (
		activityName varchar(50) not null,
		currentRate tinyInt null,
		previousRate tinyInt null,
		Improvement real null,
		primary key (activityName )
	) 
	insert into  attractionRate select 	activityName=v.activityname, 
										currentRate =v.avgcustomerRate, 
									 	previousRate = (select top 1 rateByDate.previousRate
													  from dbo.previousRate(v.activityName, @fromDate,@toDate) as rateByDate  
														 order by rateByDate.LastorderDT DESC ),
           Improvement = case when 
          (select top 1 rateByDate.previousRate
           from dbo.previousRate(v.activityName, @fromDate ,@todate) as rateByDate  
	       order by rateByDate.LastorderDT DESC ) != 0 then
					   cast(cast(v.avgcustomerRate as int) - 
                       (select top 1 rateByDate.previousRate
                       from dbo.previousRate(v.activityName, @fromDate,@toDate) as rateByDate  
                        order by rateByDate.LastorderDT DESC ) as real) / 
         (select top 1 rateByDate.previousRate
          from dbo.previousRate(v.activityName, @fromDate,@todate) as rateByDate  			
          order by rateByDate.LastorderDT DESC )
					else 0 end
	from  v_Attractions as v

select *
from v_ans



-----function
--drop function previousRate
create function previousRate (@activityName varchar(50), @fromDate datetime, @toDate datetime)
returns table
as return
		select   LastorderDT = o.orderDT, previousRate = a.customerRate
		FROM    Includes AS I JOIN Orders AS O ON O.OrdersID=I.OrdersID JOIN Purchases AS P
		ON P.PurchaseID = I.PurchaseID JOIN Attractions AS A ON A.PurchaseID=P.PurchaseID
		where A.activityName = @activityName and (o.orderDT between @fromDate and 
                   dateadd(day, -1, @toDate)
				   				   )

-------------view
--drop view v_ans
create view v_ans as
select *
from attractionRate as s
where s.Improvement != 0
-------------------------------
--drop view v_Attractions
create view v_Attractions as
select a.Activityname,avgcustomerRate=AVG(a.customerRate)
from Includes AS I JOIN Orders AS O ON O.OrdersID=I.OrdersID JOIN Purchases AS P
	ON P.PurchaseID = I.PurchaseID JOIN Attractions AS A ON A.PurchaseID=P.PurchaseID
	GROUP by a.Activityname



---מימוש
execute sp_Rate '2004-06-01 00:00:00.000', '2006-09-01 00:00:00.000'


---------------------------------------------------------------------------------------------------

with revenueByCountry as ( select c.Country, REVENUE =SUM(p.price)
                           from 	SEARCHES AS S JOIN CUSTOMERS AS C ON S.EMAIL=C.EMAIL 
			JOIN Orders AS O ON O.IPAddress = S.IPAddress AND O.SearchDT = S.SearchDT 
			JOIN Includes AS I ON I.OrdersID= O.OrdersID
	       JOIN Purchases AS P ON I.PurchaseID= P.PurchaseID
		   where year (o.orderDT)= '2022' and O.ordersID= I.ORDERSID AND P.PURCHASEID= I.PURCHASEID
		   group by C.Country  ),
		   Improve as ( select x.Country ,Improve = CASE WHEN x.revenue< y.revenue THEN 1 ELSE 0 END
              from   (select c.Country, REVENUE=sum(p.price)
                           from 	SEARCHES AS S JOIN CUSTOMERS AS C ON S.EMAIL=C.EMAIL 
			JOIN Orders AS O ON O.IPAddress = S.IPAddress AND O.SearchDT = S.SearchDT 
			JOIN Includes AS I ON I.OrdersID= O.OrdersID
	       JOIN Purchases AS P ON I.PurchaseID= P.PurchaseID
		   where year (o.orderDT)= '2021' and O.ordersID= I.ORDERSID AND P.PURCHASEID= I.PURCHASEID
		   group by c.country ) as x join 
		   ( select *
		   from  revenueByCountry 
		   ) as y on x.Country= y.Country  ),
		   
		      NumberOfOrders  AS (SELECT C.Country ,  NumberOfOrders = count(*)
                          From   SEARCHES AS S JOIN CUSTOMERS AS C ON S.EMAIL=C.EMAIL 
			                     JOIN ORDERS AS O ON O.IPAddress = S.IPAddress AND O.SearchDT = S.SearchDT
                          WHERE		year(O.OrderDT) = '2022'
                         GROUP BY	C.Country 
						 ),
						  

               AvgPriceByCustomer AS ( select M.country,  AvgPriceByCustomer= (N.REVENUE / [Num Of Customer])
					   
					 From  (SELECT C.Country, [Num Of Customer]= count ( distinct O.email)
                          From   SEARCHES AS S JOIN CUSTOMERS AS C ON S.EMAIL=C.EMAIL 
			JOIN Orders AS O ON O.IPAddress = S.IPAddress AND O.SearchDT = S.SearchDT 
			JOIN Includes AS I ON I.OrdersID= O.OrdersID
			             where year(O.OrderDT) = '2022'
	                     GROUP BY	C.Country ) as M join 

						 ( select *
		                  from  revenueByCountry 
		                    ) as N on M.Country= N.Country )
						

 -----הדוח-------------------------
 
SELECT	Country = RBC.country,
         Revenue= RBC.Revenue,
         Improve=I.Improve ,
		 [Num Of Orders]= N.NumberOfOrders, 
		 [Avg Price By Customer]=A. AvgPriceByCustomer
FROM	revenueByCountry AS RBC JOIN NumberOfOrders as N on RBC.Country= N.Country
        JOIN Improve AS I on I.country=RBC.COUNTRY
		JOIN AvgPriceByCustomer AS A on A.country = RBC.country


		----------------------------------------------