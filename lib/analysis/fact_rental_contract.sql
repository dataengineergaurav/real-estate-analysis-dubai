-- =========================
-- fact_rental_contract
-- =========================
SELECT
    contract_id,
    line_number,
    contract_reg_type_id      AS contract_type_key,
    ejari_property_type_id    AS property_key,
    area_id                   AS location_key,
    tenant_type_id            AS tenant_key,
    (EXTRACT(YEAR FROM contract_start_date) * 10000 + EXTRACT(MONTH FROM contract_start_date) * 100 + EXTRACT(DAY FROM contract_start_date)) AS start_date_key,
    (EXTRACT(YEAR FROM contract_end_date) * 10000 + EXTRACT(MONTH FROM contract_end_date) * 100 + EXTRACT(DAY FROM contract_end_date))   AS end_date_key,
    contract_amount,
    annual_amount,
    no_of_prop,
    is_free_hold
FROM rent_contracts_df;
