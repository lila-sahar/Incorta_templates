-- Author: Lila Sahar
-- Title: "Pre-Processed Data"

-- 1.0 PREPROCESS DATA -----

-- 1.1: Cleaning Table -----

SELECT
    OSV.ProductID,
    OSV.ProductName,
    OSV.OrderQuantity,
    OSV.UnitPrice,
    OSV.StandardCost,
    (OSV.OrderQuantity * OSV.UnitPrice) Revenue
FROM
    Price_Elasticity.OnlineStoreView OSV
-- check: products are available every year of the dataset
WHERE
    OSV.ProductID IN (
        SELECT
            SUB.ProductID
        FROM
            (
                SELECT
                    ProductID,
                    COUNT(
                        DISTINCT YEAR(OrderDate)
                    ) YearCount
                FROM
                    Price_Elasticity.OnlineStoreView
                WHERE 
                    1=1
                GROUP BY
                    ProductID
            ) SUB
        WHERE
            SUB.YearCount = 4
    )