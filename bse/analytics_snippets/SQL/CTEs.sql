-- CTEs: How to avoid creating intermediate tables and NOT using subqueries.
-- Check this link for reference: https://www.essentialsql.com/introduction-common-table-expressions-ctes/

--a quick example
DROP TABLE IF EXISTS sandbox_ana.example;
CREATE TABLE sandbox_ana.example AS(

WITH dates AS (SELECT dimdateid, dimdate FROM mart.dim_date),
      brands AS (SELECT dimsubbrandid, ibrandlabel FROM mart.dim_subbrand)

SELECT dates.dimdate AS date,
        brands.ibrandlabel AS brand,
        SUM(fo.quantity) AS gis #disregard the fact i didnt add filters
FROM mart.fact_orderline AS fo
    JOIN dates ON fo.dimorderlinedateid=dates.dimdateid
    JOIN brands ON fo.dimsubbrandid=brands.dimsubbrandid
GROUP BY 1,2
ORDER BY brand,date);