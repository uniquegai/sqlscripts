-- Switch to target database
USE reporting_db;

-- Step 1: Create temporary staging table for filtered orders
CREATE TEMPORARY TABLE TEMP_FILTERED_ORDERS AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.warehouse_id,
    o.status,
    o.total_amount
FROM operations_db.orders o
WHERE o.order_date BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                      AND DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day'
  AND o.status IN ('SHIPPED', 'DELIVERED');

-- Step 2: Create temporary table for item-level detail joined with parts master
CREATE TEMPORARY TABLE TEMP_ORDER_LINE_DETAIL AS
SELECT
    f.order_id,
    l.line_id,
    l.part_id,
    l.quantity,
    l.unit_price,
    l.discount,
    pm.category,
    pm.supplier_id
FROM TEMP_FILTERED_ORDERS f
JOIN operations_db.order_lines l ON f.order_id = l.order_id
LEFT JOIN inventory_db.part_master pm ON l.part_id = pm.part_id;

-- Step 3: Aggregate part-level sales volume per category
CREATE TEMPORARY TABLE TEMP_CATEGORY_SALES AS
SELECT
    category,
    SUM(quantity) AS total_units_sold,
    SUM(quantity * unit_price * (1 - discount)) AS total_revenue
FROM TEMP_ORDER_LINE_DETAIL
GROUP BY category;

-- Step 4: Supplier contribution to revenue
CREATE TEMPORARY TABLE TEMP_SUPPLIER_REVENUE AS
SELECT
    supplier_id,
    SUM(quantity * unit_price * (1 - discount)) AS supplier_revenue
FROM TEMP_ORDER_LINE_DETAIL
GROUP BY supplier_id;

-- Step 5: Join with supplier details
CREATE TEMPORARY TABLE TEMP_SUPPLIER_DETAIL AS
SELECT
    s.supplier_id,
    s.supplier_name,
    r.supplier_revenue,
    s.country,
    CASE 
        WHEN s.rating >= 4 THEN 'Preferred'
        ELSE 'Standard'
    END AS supplier_tier
FROM TEMP_SUPPLIER_REVENUE r
JOIN supplier_db.suppliers s ON r.supplier_id = s.supplier_id;

-- Step 6: Join all and prepare for loading to reporting
CREATE TEMPORARY TABLE TEMP_KPI_DATA AS
SELECT
    NOW() AS report_generated_date,
    f.order_id,
    f.order_date,
    f.warehouse_id,
    f.customer_id,
    d.part_id,
    d.quantity,
    d.unit_price,
    d.discount,
    d.category,
    d.supplier_id,
    s.supplier_name,
    s.supplier_tier,
    s.country,
    c.total_units_sold,
    c.total_revenue
FROM TEMP_ORDER_LINE_DETAIL d
JOIN TEMP_FILTERED_ORDERS f ON d.order_id = f.order_id
LEFT JOIN TEMP_SUPPLIER_DETAIL s ON d.supplier_id = s.supplier_id
LEFT JOIN TEMP_CATEGORY_SALES c ON d.category = c.category;

-- Step 7: Insert into final reporting table
INSERT INTO reporting_db.monthly_kpi_fact (
    report_generated_date,
    order_id,
    order_date,
    warehouse_id,
    customer_id,
    part_id,
    quantity,
    unit_price,
    discount,
    category,
    supplier_id,
    supplier_name,
    supplier_tier,
    country,
    total_units_sold,
    total_revenue
)
SELECT * FROM TEMP_KPI_DATA;

-- Step 8: Clean up temp tables
DROP TABLE IF EXISTS TEMP_FILTERED_ORDERS;
DROP TABLE IF EXISTS TEMP_ORDER_LINE_DETAIL;
DROP TABLE IF EXISTS TEMP_CATEGORY_SALES;
DROP TABLE IF EXISTS TEMP_SUPPLIER_REVENUE;
DROP TABLE IF EXISTS TEMP_SUPPLIER_DETAIL;
DROP TABLE IF EXISTS TEMP_KPI_DATA;
