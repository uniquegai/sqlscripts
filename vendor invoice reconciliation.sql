-- Set schema for the reconciliation process
ALTER SESSION SET CURRENT_SCHEMA = finance_ops;

-- Step 1: Extract payable invoices from last 3 months
CREATE GLOBAL TEMPORARY TABLE temp_invoices ON COMMIT PRESERVE ROWS AS
SELECT
    inv.invoice_id,
    inv.vendor_id,
    inv.invoice_date,
    inv.due_date,
    inv.amount AS invoice_amount,
    inv.currency,
    ven.vendor_name,
    inv.status
FROM ap_schema.vendor_invoices inv
JOIN ap_schema.vendors ven ON inv.vendor_id = ven.vendor_id
WHERE inv.invoice_date >= ADD_MONTHS(SYSDATE, -3)
  AND inv.status IN ('POSTED', 'PENDING');

-- Step 2: Identify payments that are linked or suspected to be linked via memo or audit logs
CREATE GLOBAL TEMPORARY TABLE temp_possible_payments ON COMMIT PRESERVE ROWS AS
SELECT
    pay.payment_id,
    pay.vendor_id,
    pay.payment_date,
    pay.amount AS payment_amount,
    pay.reference_invoice_id,
    pay.currency,
    CASE
        WHEN pay.reference_invoice_id IS NULL THEN 'UNLINKED'
        ELSE 'LINKED'
    END AS payment_status
FROM bank_schema.payments pay
WHERE pay.payment_date >= ADD_MONTHS(SYSDATE, -3)
  AND pay.status = 'CONFIRMED';

-- Step 3: Audit log extraction for investigation of manually linked invoices
CREATE GLOBAL TEMPORARY TABLE temp_audit_links ON COMMIT PRESERVE ROWS AS
SELECT DISTINCT
    a.user_id,
    a.invoice_id,
    a.payment_id,
    a.action_timestamp,
    a.remarks,
    a.action_type
FROM audit_schema.transaction_audit a
WHERE a.action_type IN ('MANUAL_LINK', 'CORRECTION')
  AND a.action_timestamp >= ADD_MONTHS(SYSDATE, -3);

-- Step 4: Complex reconciliation with prioritization
CREATE GLOBAL TEMPORARY TABLE temp_reconciliation_logic ON COMMIT PRESERVE ROWS AS
SELECT
    i.invoice_id,
    i.vendor_id,
    i.invoice_date,
    i.due_date,
    i.invoice_amount,
    i.currency,
    i.status AS invoice_status,
    p.payment_id,
    p.payment_date,
    p.payment_amount,
    a.user_id AS audit_user,
    a.action_type AS audit_action,
    CASE
        WHEN p.reference_invoice_id = i.invoice_id THEN 'AUTO_MATCH'
        WHEN a.invoice_id = i.invoice_id AND a.payment_id = p.payment_id THEN 'MANUAL_MATCH'
        ELSE 'UNMATCHED'
    END AS match_type,
    ROW_NUMBER() OVER (PARTITION BY i.invoice_id ORDER BY p.payment_date DESC) AS payment_priority
FROM temp_invoices i
LEFT JOIN temp_possible_payments p ON i.vendor_id = p.vendor_id
LEFT JOIN temp_audit_links a ON i.invoice_id = a.invoice_id AND p.payment_id = a.payment_id;

-- Step 5: Final match set where payment_priority is highest
CREATE GLOBAL TEMPORARY TABLE temp_final_matches ON COMMIT PRESERVE ROWS AS
SELECT *
FROM temp_reconciliation_logic
WHERE payment_priority = 1;

-- Step 6: Insert final match results into reconciliation table
INSERT INTO finance_ops.reconciliation_report (
    report_run_date,
    invoice_id,
    vendor_id,
    invoice_date,
    due_date,
    invoice_amount,
    currency,
    invoice_status,
    payment_id,
    payment_date,
    payment_amount,
    audit_user,
    audit_action,
    match_type
)
SELECT
    SYSDATE,
    invoice_id,
    vendor_id,
    invoice_date,
    due_date,
    invoice_amount,
    currency,
    invoice_status,
    payment_id,
    payment_date,
    payment_amount,
    audit_user,
    audit_action,
    match_type
FROM temp_final_matches;

-- Step 7: Cleanup
TRUNCATE TABLE temp_invoices;
TRUNCATE TABLE temp_possible_payments;
TRUNCATE TABLE temp_audit_links;
TRUNCATE TABLE temp_reconciliation_logic;
TRUNCATE TABLE temp_final_matches;
