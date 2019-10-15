-- Command to add JAR file provided below is a must before any external table creation
-- IMPORTANT: BEFORE CREATING ANY TABLE, MAKE SURE YOU RUN THIS COMMAND 
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar;

-- Drop external table Hive metastore structure 
drop table if exists harshendra_pgddsc8.nyc_yellow_taxi ; 

-- CREATE EXTERNAL TABLE 
-- Command explained: 
-- 1) Read csv file hence "terminated by ','"
-- 2) Skip header record hence the last line 'tblproperties ("skip.header.line.count"="1");'
-- 3) Location of the file is '/common_folder/nyc_taxi_data/'

CREATE external TABLE
  IF NOT EXISTS harshendra_pgddsc8.nyc_yellow_taxi(
    vendorid INT, tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count INT, trip_distance DOUBLE,
    ratecodeid      INT, store_and_fwd_flag string,
    pulocationid    INT, dolocationid INT,
    payment_type    INT, fare_amount DOUBLE,
    extra DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE,
    tolls_amount DOUBLE, improvement_surcharge DOUBLE,
    total_amount DOUBLE
    ) row format delimited fields TERMINATED BY ',' location '/common_folder/nyc_taxi_data/' tblproperties ("skip.header.line.count" = "1") ;

----------------------------------------------------------------------------
-- Perform EDA , basic quality checks and identify bad records if any 
---------------------------------------------------------
--    Basic Data Quality Checks                  
---------------------------------------------------------
-- 1) How many records has each TPEP provider provided? Write a query that summarises the number of records of each provider.

SELECT count(1) as record_count,
       ( CASE
           WHEN vendorid = 1 THEN 'Creative Mobile Technologies, LLC'
           WHEN vendorid = 2 THEN 'VeriFone Inc'
         END ) AS TPEP_provider
FROM   harshendra_pgddsc8.nyc_yellow_taxi
GROUP  BY ( CASE
              WHEN vendorid = 1 THEN 'Creative Mobile Technologies, LLC'
              WHEN vendorid = 2 THEN 'VeriFone Inc'
            END )  ;
            

-- record_count        tpep_provider
-- 527,386	            Creative Mobile Technologies, LLC
-- 647,183	            VeriFone Inc

-- ( total of this comes to 1,174,569 records , split-up of which is provided above )


-- 2) The data provided is for months November and December only. Check whether the data is consistent, and if not, identify the data quality issues. 
-- Mention all data quality issues in comments.
-- a) as per assignment "In this assignment, we ONLY consider the data of yellow taxis for November and December of the year 2017." , so let's validate this : 



SELECT Count(1) AS record_count,
       Date_format(tpep_pickup_datetime, 'yyyy') AS yr,
       Date_format(tpep_pickup_datetime, 'MM')   AS mnth,
       ( CASE
           WHEN vendorid = 1 THEN 'Creative Mobile Technologies, LLC'
           WHEN vendorid = 2 THEN 'VeriFone Inc'
         end )                                   AS TPEP_provider
FROM   harshendra_pgddsc8.nyc_yellow_taxi
GROUP  BY Date_format(tpep_pickup_datetime, 'yyyy'),
          Date_format(tpep_pickup_datetime, 'MM'),
          ( CASE
              WHEN vendorid = 1 THEN 'Creative Mobile Technologies, LLC'
              WHEN vendorid = 2 THEN 'VeriFone Inc'
            end )
ORDER  BY yr DESC,
          mnth DESC; 

--  record_count	yr	    mnth	tpep_provider
--	4	            2018	01	    VeriFone Inc
--	328151	        2017	12  	VeriFone Inc
--	266104      	2017	12  	Creative Mobile Technologies, LLC
--	319018	        2017	11	    VeriFone Inc
--	261282	        2017	11	    Creative Mobile Technologies, LLC
--	6	            2017	10	    VeriFone Inc
--	1	            2009	01	    VeriFone Inc
--	2	            2008	12	    VeriFone Inc
--	1	            2003	01	    VeriFone Inc

-- i) From pickup datetime ,  as seen above, we have records of 2018 /2009/2003 January and 2008 December
-- ii) Also as can be seen , these records are  handful ( 14) and supplied by  "VeriFone Inc" TPEP Vendor 
-- let's check drop date and day to see if we have anyone not in November and December 2017. 
-- One edge case would be pick up on 31st December, and so the drop date would be January 1st 2018, these rows are ok. let's check if there are such rows :

SELECT Count(1)  AS record_count,
       Date_format(tpep_pickup_datetime, 'yyyy')  AS yr_pickup,
       Date_format(tpep_pickup_datetime, 'MM')    AS mnth_pickup,
       Date_format(tpep_pickup_datetime, 'dd')    AS day_pickup,
       Date_format(tpep_dropoff_datetime, 'yyyy') AS yr_drop,
       Date_format(tpep_dropoff_datetime, 'MM')   AS mnth_drop,
       Date_format(tpep_dropoff_datetime, 'dd')   AS day_drop,
       ( CASE
           WHEN vendorid = 1 THEN 'Creative Mobile Technologies, LLC'
           WHEN vendorid = 2 THEN 'VeriFone Inc'
         end )                                    AS TPEP_provider
FROM   harshendra_pgddsc8.nyc_yellow_taxi
WHERE  ( Date_format(tpep_pickup_datetime, 'MM') NOT IN ( 11, 12 )
          OR Date_format(tpep_pickup_datetime, 'yyyy') NOT IN ( 2017 ) )
       AND ( Date_format(tpep_dropoff_datetime, 'MM') NOT IN ( 11, 12 )
              OR Date_format(tpep_dropoff_datetime, 'yyyy') NOT IN ( 2017 ) )
GROUP  BY Date_format(tpep_pickup_datetime, 'yyyy'),
          Date_format(tpep_pickup_datetime, 'MM'),
          Date_format(tpep_pickup_datetime, 'dd'),
          Date_format(tpep_dropoff_datetime, 'yyyy'),
          Date_format(tpep_dropoff_datetime, 'MM'),
          Date_format(tpep_dropoff_datetime, 'dd'),
          ( CASE
              WHEN vendorid = 1 THEN 'Creative Mobile Technologies, LLC'
              WHEN vendorid = 2 THEN 'VeriFone Inc'
            end )
ORDER  BY yr_drop DESC,
          mnth_drop DESC,
          day_drop ASC; 


-- 	record_count	yr_pickup	mnth_pickup	day_pickup	yr_drop	mnth_drop	day_drop	tpep_provider
--	4	            2018	        01	        01	    2018	    01	        01	    VeriFone Inc
--	2	            2017	        10	        31	    2017	    10	        31	    VeriFone Inc
--	1	            2009	        01	        01	    2009	    01	        01	    VeriFone Inc
--	1	            2008	        12	        31	    2009	    01      	01	    VeriFone Inc
--	1	            2008	        12	        31	    2008	    12	        31	    VeriFone Inc
--	1	            2003	        01	        01	    2003	    01	        01	    VeriFone Inc

-- These 10 records have pickup/drop that don't match the expected date and should be dropped 


-- 3) You might have encountered unusual or erroneous rows in the dataset. Can you conclude which vendor is doing a bad job in providing the records using different 
-- columns of the dataset? 
-- Summarise your conclusions based on every column where these errors are present. For example,  There are unusual passenger count, i.e. 0 which is unusual.
-- HINT: Use the Data Dictionary provided to validate the data present in the records provided.

-- iii) Let's check if we have "Null" passenger count or instances where passenger_count = 0; 

SELECT Count(1) AS record_count,
       passenger_count,
       ( CASE
           WHEN vendorid = 1 THEN 'Creative Mobile Technologies, LLC'
           WHEN vendorid = 2 THEN 'VeriFone Inc'
         end )  AS TPEP_provider
FROM   harshendra_pgddsc8.nyc_yellow_taxi
WHERE  passenger_count IS NULL
        OR passenger_count = 0
GROUP  BY passenger_count,
          ( CASE
              WHEN vendorid = 1 THEN 'Creative Mobile Technologies, LLC'
              WHEN vendorid = 2 THEN 'VeriFone Inc'
            end )
ORDER  BY passenger_count ASC; 


-- 	record_count	passenger_count	tpep_provider
-- 	11	            0	            VeriFone Inc
--	6813	        0	            Creative Mobile Technologies, LLC

-- as seen above Creative Mobile passed 6,813 records having passenger_count = 0, these rows don't help us and needs to be removed

-- iv) Let's check Trip distance 
-- ASSUMPTION : There can be round trips; so Pick up and drop-off location will be the same. You board the cab and go to wherever  you want and then come back
--              to the same place. So we ONLY CONSIDER all the rows where Drop off and pickup points are the same AND where trip_distance > "0" 
--              hence the only condition we check is if trip_distance = 0 or null and then drop those records from analysis 

SELECT Count(1) AS trip_0_count
FROM   harshendra_pgddsc8.nyc_yellow_taxi
WHERE  ( trip_distance = 0
          OR trip_distance IS NULL ); 
-- 	trip_0_count
--	7402
-- These rows will not be considered for analysis 

-- let's pull vendor-wise count for Trip_distance = 0 (bad records)

SELECT Count(1) AS trip_0_count,
       ( CASE
           WHEN vendorid = 1 THEN 'Creative Mobile Technologies, LLC'
           WHEN vendorid = 2 THEN 'VeriFone Inc'
         end )  AS TPEP_provider
FROM   harshendra_pgddsc8.nyc_yellow_taxi
WHERE  ( trip_distance = 0
          OR trip_distance IS NULL )
GROUP  BY ( CASE
              WHEN vendorid = 1 THEN 'Creative Mobile Technologies, LLC'
              WHEN vendorid = 2 THEN 'VeriFone Inc'
            end );          
-- 	trip_0_count	tpep_provider
--	4217	        Creative Mobile Technologies, LLC
--	3185	        VeriFone Inc




-- v) let's inspect RateCodeID 

SELECT Count(1) AS rate_code_count,
       ratecodeid,
       ( CASE
           WHEN vendorid = 1 THEN 'Creative Mobile Technologies, LLC'
           WHEN vendorid = 2 THEN 'VeriFone Inc'
         end )  AS TPEP_provider
FROM   harshendra_pgddsc8.nyc_yellow_taxi
WHERE  ratecodeid NOT IN ( 1, 2, 3, 4,
                           5, 6 )
GROUP  BY ratecodeid,
          ( CASE
              WHEN vendorid = 1 THEN 'Creative Mobile Technologies, LLC'
              WHEN vendorid = 2 THEN 'VeriFone Inc'
            end )
ORDER  BY ratecodeid ASC; 

-- 1= Standard rate
-- 2=JFK
-- 3=Newark
-- 4=Nassau or Westchester
-- 5=Negotiated fare
-- 6=Group ride

-- 	rate_code_count	ratecodeid  	tpep_provider
--	1               	99      	VeriFone Inc
--	8	                99	        Creative Mobile Technologies, LLC

-- As can be seen, the 9 records above does not satisfy  any rate card as mentioned in the list and should be removed 

-- vi) Let's inspect store_and_fwd_flag 

SELECT Count(1) AS s_n_f_count,
       store_and_fwd_flag
FROM   harshendra_pgddsc8.nyc_yellow_taxi
GROUP  BY store_and_fwd_flag
ORDER  BY store_and_fwd_flag ASC; 

--  	s_n_f_count	store_and_fwd_flag
--  	1170618	        N
--  	3951        	Y
-- as seen above, Store_and_fwd_flag column is good

-- vii) Let's inspect Payment_type
SELECT Count(1) AS payment_type_count,
       payment_type
FROM   harshendra_pgddsc8.nyc_yellow_taxi
GROUP  BY payment_type
ORDER  BY payment_type ASC; 

--  1= Credit card
--  2= Cash
--  3= No charge
--  4= Dispute
--  5= Unknown
--  6= Voided trip

-- 	payment_type_count	payment_type
--	790256	            1
--	376374	            2
--	6274	            3
--	1665            	4

-- as seen above, Payment_type column looks good 

-- viii) Let's inspect Fare_amount column 

SELECT Min (fare_amount) AS minimum_Fare_amount,
       Max(fare_amount)  AS maximum_Fare_amount
FROM   harshendra_pgddsc8.nyc_yellow_taxi; 

-- 	minimum_fare_amount	maximum_fare_amount
--  	-200            	650

SELECT Count(1) AS fare_rec_count
FROM   harshendra_pgddsc8.nyc_yellow_taxi
WHERE  fare_amount <= 0; 
  -- 870 records
  
  -- as can be seen these records don't contribute much towards the analysis and should to be removed
  
SELECT Count(1) AS Total_amount
FROM   harshendra_pgddsc8.nyc_yellow_taxi
WHERE  total_amount <= 0; 
  -- 681 records

SELECT Count(1) AS extra_count,  
       extra
FROM   harshendra_pgddsc8.nyc_yellow_taxi
where extra < 0 
GROUP  BY extra
ORDER  BY extra ASC; 

-- 	extra_count	    extra
--	1           	-10.6
--	5	            -4.5
--	87          	-1
--	193         	-0.5
-- Dictionary  says : "Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges."
-- Assumption: "Negative values" does not constitute extras and we plan to drop them 

----------------------------------------------------------------------------
 -- Estimate total record count of faulty records and categorize the data vendor-wise
----------------------------------------------------------------------------
SELECT Count(1) AS faulty_records,
       ( CASE
           WHEN vendorid = 1 THEN 'Creative Mobile Technologies, LLC'
           WHEN vendorid = 2 THEN 'VeriFone Inc'
         end )  AS TPEP_provider
FROM   harshendra_pgddsc8.nyc_yellow_taxi
WHERE  ( ( Date_format(tpep_pickup_datetime, 'MM') NOT IN ( 11, 12 )
            OR Date_format(tpep_pickup_datetime, 'yyyy') NOT IN ( 2017 ) )
         AND ( Date_format(tpep_dropoff_datetime, 'MM') NOT IN ( 11, 12 )
                OR Date_format(tpep_dropoff_datetime, 'yyyy') NOT IN ( 2017 ) )
       )
        OR passenger_count IS NULL
        OR passenger_count = 0 
        OR trip_distance = 0
        OR trip_distance IS NULL 
        OR extra < 0 
        OR ( ratecodeid NOT IN ( 1, 2, 3, 4,
                                 5, 6 ) )
        OR ( fare_amount <= 0 )
GROUP  BY ( CASE
              WHEN vendorid = 1 THEN 'Creative Mobile Technologies, LLC'
              WHEN vendorid = 2 THEN 'VeriFone Inc'
            end ); 
-- 	faulty_records	tpep_provider
--	11084	        Creative Mobile Technologies, LLC
--	3670	        VeriFone Inc

-------------------------------------------------------------------------------------------------------------
-- Result  : "Creative Mobile Technologies, LLC" vendor is sending bad quality records more than  "VeriFone Inc" 
----------------------------------------------------------------------------------------------------------------
-- Create ORC file format and import cleaned data ; Exclude faulty records 
-- ASSUMPTIONS: i ) For all the analysis, we are considering people who travelled using Yellow taxi in the month of November and December 2017
--            ii) We are choosing those records wherein number of passengers (passenger_count) is Greater than '0'
--           iii) We are choosing those records where trip distance is Greater than '0' , which means the taxi ran for a certain distance 
--            iv) Final RateCodeID is one among the ones mentioned in dictionary (1,2,3,4,5,6)
--             v) Fare amount is Greater than 0 

-- IMPORTANT: Before partitioning any table, make sure you run the below commands.

SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

drop table if exists harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned; 

CREATE external TABLE
  IF NOT EXISTS harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned
    (
    vendorid INT, tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count INT, trip_distance DOUBLE,
    ratecodeid      INT, store_and_fwd_flag string,
    pulocationid    INT, dolocationid INT,
    payment_type    INT, fare_amount DOUBLE,
    extra DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE,
    tolls_amount DOUBLE, improvement_surcharge DOUBLE,
    total_amount DOUBLE
    ) partitioned BY ( mnth INT , dy INT)
    stored
  AS
    orc location '/user/hive/warehouse/harshendra_pgddsc8_nyc_orc'
    tblproperties ("orc.compress"="SNAPPY");


 INSERT overwrite table harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned partition
       (
              mnth ,
              dy
       )
SELECT vendorid ,
       tpep_pickup_datetime,
       tpep_dropoff_datetime,
       passenger_count,
       trip_distance,
       ratecodeid ,
       store_and_fwd_flag ,
       pulocationid ,
       dolocationid ,
       payment_type ,
       fare_amount ,
       extra ,
       mta_tax ,
       tip_amount ,
       tolls_amount ,
       improvement_surcharge ,
       total_amount ,
       date_format(tpep_pickup_datetime , 'MM') AS mnth,
       date_format(tpep_pickup_datetime , 'dd') AS dy
FROM   harshendra_pgddsc8.nyc_yellow_taxi
WHERE  ( (
                     date_format(tpep_pickup_datetime, 'MM')   IN (11,12)
              AND    date_format(tpep_pickup_datetime, 'yyyy') IN (2017))
       AND    (
                     date_format(tpep_dropoff_datetime, 'MM')   IN (11,12)
              AND    date_format(tpep_dropoff_datetime, 'yyyy') IN (2017)) )
AND    passenger_count > 0
AND    trip_distance > 0
AND    extra >= 0
AND    ratecodeid IN (1,2,3,4,5,6)
AND    fare_amount > 0 ;




-- ####################################################################################
-- Analysis-I
-- ####################################################################################


-- 1) Compare the overall average fare per trip for November and December.

SELECT Round (Avg(total_amount), 2) AS avg_fare_trip,
       ( CASE
           WHEN mnth = 11 THEN 'November'
           WHEN mnth = 12 THEN 'December'
         end )                      AS month_of_trip
FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned
GROUP  BY ( CASE
              WHEN mnth = 11 THEN 'November'
              WHEN mnth = 12 THEN 'December'
            end )
ORDER  BY month_of_trip DESC; 

-- 	avg_fare_trip	month_of_trip
--	16.41       	November
--	16.1           	December

-- Average fare per trip is around $16.41 in November and $16.10 in December (rounded off to two decimal places) 

-- 2) Explore the 'number of passengers per trip' - how many trips are made by each level of 'Passenger_count'? Do most people travel solo or with other people?

SELECT Count(1) AS trip_count,
       passenger_count
FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned
GROUP  BY passenger_count
ORDER  BY trip_count DESC; 

-- 	trip_count	passenger_count
--  821239      	1
--	175833	        2
--	54331       	5
--	50453       	3
--	33032       	6
--	24813       	4
--	3           	7

-- Result:  Details of trip count for 'Passengers per trip' given above. Yes, most people prefer to travel solo, (about 70.81%) 
-- More elaborate query with percentage is given below:


SELECT a.trip_count  AS trips,
       a.passenger_count   AS passengers,
       Round(100 * ( a.trip_count / b.total_trip_count ), 4) AS percentage
FROM   (SELECT Count(1) AS trip_count,
               passenger_count
        FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned
        GROUP  BY passenger_count)a,
       (SELECT Count(1) AS total_trip_count
        FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned)b
ORDER  BY trips DESC; 

-- 	trips	passengers	percentage
--	821239  	1          70.8145
--	175833  	2          15.1619
--	54331   	5       	4.6849
--	50453   	3       	4.3505
--	33032   	6       	2.8483
--	24813   	4       	2.1396
--	3	        7       	0.0003


-- 3) Which is the most preferred mode of payment?

SELECT ( CASE
           WHEN a.payment_type = 1 THEN 'Credit Card'
           WHEN a.payment_type = 2 THEN 'Cash'
           WHEN a.payment_type = 3 THEN 'No Charge'
           WHEN a.payment_type = 4 THEN 'Dispute'
           WHEN a.payment_type = 5 THEN 'Unknown'
           WHEN a.payment_type = 6 THEN 'Voided trip'
           ELSE 'Invalid entry'
         end )   AS payment_type,
       a.trip_count     AS trips,
       Round(100 * ( a.trip_count / b.total_count ), 4) AS percentage
FROM   (SELECT Count(1) AS trip_count,
               payment_type
        FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned
        GROUP  BY payment_type)a,
       (SELECT Count(1) AS total_count
        FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned)b
ORDER  BY trips DESC ;

-- 	payment_type    	trips   	percentage
--	Credit Card	        782750	    67.4957
--	Cash	            370844  	31.9774
--	No Charge          	4782    	0.4123
--	Dispute         	1328    	0.1145

-- As seen above, Credit card is the most preferred payment type in most of the trips (67.5% ) 


-- 4) What is the average tip paid per trip? Compare the average tip with the 25th, 50th and 75th percentiles 
--    and comment whether the 'average tip' is a representative statistic (of the central tendency) of 'tip amount paid'. 
--    Hint: You may use percentile_approx(DOUBLE col, p): Returns an approximate pth percentile of a numeric column (including floating point types) in the group.

-- Cash tips are not considered for the same as they appear as 0$, i.e. payment_Type = 2  is excluded from analysis

SELECT Round(Avg(tip_amount), 2)  AS average_tip_amount,
       Round(Percentile_approx(tip_amount, 0.25), 2) AS 25th_percentile,
       Round(Percentile_approx(tip_amount, 0.50), 2) AS 50th_percentile,
       Round(Percentile_approx(tip_amount, 0.75), 2) AS 75th_percentile
FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned
WHERE 
payment_type != 2 ;
-- exclude cash as tip amount does not include cash tips 

-- 	average_tip_amount  	25th_percentile	     50th_percentile	        75th_percentile
--	2.72	                 1.35	                2                   	3.06

-- Considering only those people who always tip (excluding 35% of non-tippers)
-- Average tip is 2$ and 72 Cents
-- 25th percentile is 1$ and 35 Cents
-- 50th percentile is 2$
-- 75th percentile is 3$ and 06 Cents 
-- So as per the above observation, 'Average tip' does  NOT appear to be a representative statistic of the central tendency, it is 0.72 more than 50th percentile 

-- 5) Explore the 'Extra' (charge) variable - what fraction of total trips have an extra charge is levied?

SELECT a.trip_count   AS trips,
       100 * Round (( a.trip_count / b.total_count ), 2) AS percentage,
       a.extra_levied_indicator   AS extra_charges_levied
FROM   (SELECT Count(1) AS trip_count,
               ( CASE
                   WHEN extra = 0  THEN 'N'
                   WHEN extra > 0 THEN 'Y'
                 end )  AS extra_levied_indicator
        FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned
        GROUP  BY ( CASE
                      WHEN extra = 0  THEN 'N'
                      WHEN extra > 0 THEN 'Y'
                    end ))a,
       (SELECT Count(1) AS total_count
        FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned) b; 
-- 	trips	percentage	extra_charges_levied
--	622372  	54      	N
--	537332	    46      	Y

-- As seen above, 46% of the trips have extra charges levied, 53% of trips don't have extra charges levied.

-- #############################################################################################
--      Analysis-II
-- #############################################################################################


-- 1) What is the correlation between the number of passengers on any given trip, and the tip paid per trip? 
--      Do multiple travellers tip more compared to solo travellers? Hint: Use CORR(Col_1, Col_2)
--  let's consider all the passengers : 
SELECT Round(Corr(( tip_amount ), ( passenger_count )),4)  AS corr_coeff
FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned
WHERE payment_type != 2  ; -- let's exclude Cash payment type as cash tips are not included 
-- corr_coeff
-- 0.0093
-- as seen above, correlation between number of passengers of any given trip and tip paid is 0.0093 (positive integer) excluding cash tips
-- this correlation positive integer is very small number

 SELECT Round(Avg(tip_amount), 2)                     AS Average_tip,
       Round(Percentile_approx(tip_amount, 0.50), 2) AS 50th_percentile,
       ( CASE
           WHEN passenger_count = 1 THEN 'Solo'
           WHEN passenger_count > 1 THEN 'Multiple'
           ELSE 'NA'
         END )                                       AS category
FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned
WHERE  payment_type != 2
-- let's exclude Cash payment type as cash tips are not included 
GROUP  BY ( CASE
              WHEN passenger_count = 1 THEN 'Solo'
              WHEN passenger_count > 1 THEN 'Multiple'
              ELSE 'NA'
            END );  
-- 	average_tip	     50th_percentile        category
--  2.78            	  2                  Multiple
--	2.69            	  2                  Solo

-- As seen from "average" tip ,  "Multiple passengers" appear to tip more compared to "Solo passengers"
-- However when you check 50th percentile which is representative of majority population stastistic, tip amount is 2 
-- this indicates passenger count does not have an impact on the tip amount (solo or multiple, people tip alike approximating to 2$ considering median ) 


-- 2) Segregate the data into five segments of 'tip paid': [0-5), [5-10), [10-15) , [15-20) and >=20. 
--      Calculate the percentage share of each bucket (i.e. the fraction of trips falling in each bucket).

SELECT a.record_count                                      AS trips,
       Round (100 * ( a.record_count / b.total_count ), 2) AS percentage,
       a.tip_label                                         AS tip_bucket
FROM   (SELECT Count(1) AS record_count,
               ( CASE
                   WHEN tip_amount >= 0
                        AND tip_amount < 5 THEN '0-5'
                   WHEN tip_amount >= 5
                        AND tip_amount < 10 THEN '5-10'
                   WHEN tip_amount >= 10
                        AND tip_amount < 15 THEN '10-15'
                   WHEN tip_amount >= 15
                        AND tip_amount < 20 THEN '15-20'
                   WHEN tip_amount >= 20 THEN '>=20'
                   ELSE 'invalid data'
                 end )  AS tip_label
        FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned
        GROUP  BY ( CASE
                      WHEN tip_amount >= 0
                           AND tip_amount < 5 THEN '0-5'
                      WHEN tip_amount >= 5
                           AND tip_amount < 10 THEN '5-10'
                      WHEN tip_amount >= 10
                           AND tip_amount < 15 THEN '10-15'
                      WHEN tip_amount >= 15
                           AND tip_amount < 20 THEN '15-20'
                      WHEN tip_amount >= 20 THEN '>=20'
                      ELSE 'invalid data'
                    end ))a,
       (SELECT Count(1) AS total_count
        FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned) b; 

-- 	        trips	          percentage      tip_bucket
-- 	        1069001	            92.18       	0-5
-- 	        21407	            1.85        	10-15
-- 	        2627	            0.23        	15-20
-- 	        65605           	5.66        	5-10
-- 	        1064            	0.09        	>=20



-- 3) Which month has a greater average 'speed' - November or December? Note that the variable 'speed' will have to be derived from other metrics. 
--      Hint: You have columns for distance and time.
-- speed = distance/ time 
-- fetch unix timestamp and calculate difference between drop and pickup
-- divide time by 3600 to get # hours 
-- divide distance(in miles) / time(in hours) to get speed in miles per hour (or mph)
-- Check the average mph speed for two months of interest 

SELECT Round (Avg(a.speed_mph), 2) AS avg_speed_mph,
       ( CASE
           WHEN a.mnth = 11 THEN 'November'
           WHEN a.mnth = 12 THEN 'December'
         end )                     AS month_of_trip
FROM   (SELECT mnth,
               Round(trip_distance / ( ( Unix_timestamp(tpep_dropoff_datetime) -
                                         Unix_timestamp(tpep_pickup_datetime)
                                             )
                                                             / 3600 ), 2) AS
               speed_mph
        FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned)a
GROUP  BY ( CASE
              WHEN a.mnth = 11 THEN 'November'
              WHEN a.mnth = 12 THEN 'December'
            end )
ORDER  BY month_of_trip DESC; 


-- 	avg_speed_mph	month_of_trip
--	11           	November
--	11.1          	December

-- Per observation, month of December has slightly 'greater' average speed at 11.1 mph (greater by 0.1 mph ) 




-- 4) Analyse the average speed of the most happening days of the year, i.e. 31st December 
--     (New year's eve) and 25th December (Christmas) and compare it with the overall average. 
-- speed = distance/ time 
-- fetch unix timestamp and calculate difference between drop and pickup
-- divide time by 3600 to get # hours 
-- divide distance(in miles) / time(in hours) to get speed in miles per hour (or mph)
-- compute this for the whole month of December and for the days of interest 
SELECT Round (Avg(a.speed_mph), 2) AS avg_speed_mph,
       a.dy                        AS day,
       ( CASE
           WHEN a.mnth = 11 THEN 'November'
           WHEN a.mnth = 12 THEN 'December'
         end )                     AS month_of_trip,
       b.dec_speed_mph             AS December_speed_mph
FROM   (SELECT dy,
               mnth,
               Round(trip_distance / ( ( Unix_timestamp(tpep_dropoff_datetime) -
                                         Unix_timestamp(tpep_pickup_datetime)
                                             )
                                                             / 3600 ), 2) AS
               speed_mph
        FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned
        WHERE  mnth = 12
               AND dy IN ( 25, 31 ))a,
       (SELECT Round(Avg(Round(trip_distance / ( (
                               Unix_timestamp(tpep_dropoff_datetime
                               ) -
                                             Unix_timestamp(tpep_pickup_datetime
                                             )
                                                 )
                                                                 / 3600 ), 2)),
               2) AS
               dec_speed_mph
        FROM   harshendra_pgddsc8.nyc_yellow_taxi_orc_partitioned
        WHERE  mnth = 12) b
GROUP  BY a.dy,
          ( CASE
              WHEN a.mnth = 11 THEN 'November'
              WHEN a.mnth = 12 THEN 'December'
            end ),
          b.dec_speed_mph; 


-- 	avg_speed_mph	day	        month_of_trip	december_speed_mph
--	15.27	        25	        December	        11.1
--	13.27	        31	        December        	11.1

-- As seen from the result, December average speed is 11.1 miles per hour (mph) ; 
-- speed on 25th December (Christmas eve)is 15.27 mph 
-- speed on 31st December (New year's eve) is 13.27 mph
-- both these are higher in mph than the average mph for the month of December 2017