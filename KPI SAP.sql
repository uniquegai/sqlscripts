-- Switch to relevant schema
SET SCHEMA ZSALES_ANALYTICS;

-- Step 1: Select all fulfilled sales orders from last month
CREATE LOCAL TEMPORARY TABLE #SalesOrders AS
SELECT
    VBAK.VBELN AS SalesOrder,
    VBAK.KUNNR AS CustomerID,
    VBAK.AUART AS OrderType,
    VBAK.AUDAT AS OrderDate,
    VBAP.POSNR AS Item,
    VBAP.MATNR AS Material,
    VBAP.WERKS AS Plant,
    VBAP.LFIMG AS ConfirmedQty,
    VBAP.NETWR AS NetValue,
    VBAP.WAERK AS Currency
FROM SAPABAP1.VBAK
JOIN SAPABAP1.VBAP ON VBAK.VBELN = VBAP.VBELN
WHERE VBAK.AUDAT BETWEEN ADD_MONTHS(CURRENT_DATE, -1) AND CURRENT_DATE
  AND VBAK.FKDAT IS NOT NULL;

-- Step 2: Add delivery information
CREATE LOCAL TEMPORARY TABLE #Deliveries AS
SELECT
    LIKP.VBELN AS DeliveryDoc,
    LIKP.VSTEL AS ShippingPoint,
    LIKP.LFDAT AS PlannedDeliveryDate,
    LIKP.WADAT_IST AS ActualDeliveryDate,
    LIPS.VGBEL AS SalesOrder,
    LIPS.VGPOS AS Item
FROM SAPABAP1.LIKP
JOIN SAPABAP1.LIPS ON LIKP.VBELN = LIPS.VBELN
WHERE LIKP.WADAT_IST IS NOT NULL;

-- Step 3: Join deliveries with sales orders
CREATE LOCAL TEMPORARY TABLE #SalesWithDelivery AS
SELECT
    s.SalesOrder,
    s.Item,
    s.CustomerID,
    s.Material,
    s.OrderDate,
    d.PlannedDeliveryDate,
    d.ActualDeliveryDate,
    CASE
        WHEN d.ActualDeliveryDate > d.PlannedDeliveryDate THEN 'LATE'
        ELSE 'ON_TIME'
    END AS DeliveryStatus
FROM #SalesOrders s
LEFT JOIN #Deliveries d
  ON s.SalesOrder = d.SalesOrder AND s.Item = d.Item;

-- Step 4: Join pricing and discounts from KONV
CREATE LOCAL TEMPORARY TABLE #Pricing AS
SELECT
    VBELN AS SalesOrder,
    KPOSN AS Item,
    KBETR / 10 AS DiscountAmount,
    KPEIN AS PerUnit,
    KSCHL AS ConditionType
FROM SAPABAP1.KONV
WHERE KSCHL IN ('ZDIS', 'K007')  -- discounts
  AND STUNR = '1'
  AND KRECH = 'C';

-- Step 5: Enrich with customer details
CREATE LOCAL TEMPORARY TABLE #CustomerMaster AS
SELECT
    KUNNR AS CustomerID,
    NAME1 AS CustomerName,
    LAND1 AS Country,
    KUKLA AS CustomerGroup,
    SPART AS Division
FROM SAPABAP1.KNA1;

-- Step 6: Final KPI construction
CREATE LOCAL TEMPORARY TABLE #KPIReport AS
SELECT
    s.SalesOrder,
    s.Item,
    c.CustomerName,
    c.Country,
    c.CustomerGroup,
    s.Material,
    s.OrderDate,
    s.PlannedDeliveryDate,
    s.ActualDeliveryDate,
    s.DeliveryStatus,
    p.DiscountAmount,
    o.NetValue,
    (o.NetValue - COALESCE(p.DiscountAmount, 0)) AS NetRevenue,
    CASE WHEN s.DeliveryStatus = 'LATE' THEN 1 ELSE 0 END AS LateDeliveryFlag
FROM #SalesWithDelivery s
LEFT JOIN #Pricing p ON s.SalesOrder = p.SalesOrder AND s.Item = p.Item
LEFT JOIN #SalesOrders o ON s.SalesOrder = o.SalesOrder AND s.Item = o.Item
LEFT JOIN #CustomerMaster c ON s.CustomerID = c.CustomerID;

-- Step 7: Insert into reporting table
INSERT INTO ZSALES_ANALYTICS.MONTHLY_KPI_REPORT
(
    SalesOrder, Item, CustomerName, Country, CustomerGroup,
    Material, OrderDate, PlannedDeliveryDate, ActualDeliveryDate,
    DeliveryStatus, DiscountAmount, NetValue, NetRevenue, LateDeliveryFlag
)
SELECT * FROM #KPIReport;

-- Step 8: Clean up
DROP TABLE #SalesOrders;
DROP TABLE #Deliveries;
DROP TABLE #SalesWithDelivery;
DROP TABLE #Pricing;
DROP TABLE #CustomerMaster;
DROP TABLE #KPIReport;
