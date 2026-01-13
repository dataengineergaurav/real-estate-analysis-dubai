-- =========================
-- dim_location
-- =========================
SELECT DISTINCT
    project_number,
    project_name_en,
    project_name_ar,
    master_project_en,
    master_project_ar,
    area_id,
    area_name_en,
    area_name_ar,
    actual_area,
    nearest_landmark_en,
    nearest_landmark_ar,
    nearest_metro_en,
    nearest_metro_ar,
    nearest_mall_en,
    nearest_mall_ar
FROM rent_contracts_df;