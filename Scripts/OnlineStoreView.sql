-- Author: Lila Sahar
-- Title: "OnlineStoreView"

SELECT
    CAST(p.ProductID AS STRING) AS "ProductID",
    p.ProductName AS "ProductName",
    sod.OrderQty AS "OrderQuantity",
    soh.OrderDate AS "OrderDate",
    sod.UnitPrice AS "UnitPrice",
    p.StandardCost AS "StandardCost"
FROM
    OnlineStore.salesorderdetail sod
    LEFT JOIN OnlineStore.salesorderheader soh ON sod.SalesOrderID = soh.SalesOrderID
    LEFT JOIN OnlineStore.product p ON sod.ProductID = p.ProductID