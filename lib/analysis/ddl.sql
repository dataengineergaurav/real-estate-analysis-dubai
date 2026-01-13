-- SQLite
PRAGMA foreign_keys = ON;

-- =========================
-- dim_contract_type
-- =========================
CREATE TABLE dim_contract_type (
    contract_type_key INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_reg_type_id INTEGER,
    contract_reg_type_en TEXT,
    contract_reg_type_ar TEXT
);

-- =========================
-- dim_property
-- =========================
CREATE TABLE dim_property (
    property_key INTEGER PRIMARY KEY AUTOINCREMENT,
    ejari_bus_property_type_id INTEGER,
    ejari_bus_property_type_en TEXT,
    ejari_bus_property_type_ar TEXT,
    ejari_property_type_id INTEGER,
    ejari_property_type_en TEXT,
    ejari_property_type_ar TEXT,
    ejari_property_sub_type_id INTEGER,
    ejari_property_sub_type_en TEXT,
    ejari_property_sub_type_ar TEXT,
    property_usage_en TEXT,
    property_usage_ar TEXT
);

-- =========================
-- dim_location
-- =========================
CREATE TABLE dim_location (
    location_key INTEGER PRIMARY KEY AUTOINCREMENT,
    project_number INTEGER,
    project_name_en TEXT,
    project_name_ar TEXT,
    master_project_en TEXT,
    master_project_ar TEXT,
    area_id INTEGER,
    area_name_en TEXT,
    area_name_ar TEXT,
    actual_area TEXT,
    nearest_landmark_en TEXT,
    nearest_landmark_ar TEXT,
    nearest_metro_en TEXT,
    nearest_metro_ar TEXT,
    nearest_mall_en TEXT,
    nearest_mall_ar TEXT
);

-- =========================
-- dim_tenant
-- =========================
CREATE TABLE dim_tenant (
    tenant_key INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_type_id INTEGER,
    tenant_type_en TEXT,
    tenant_type_ar TEXT
);

-- =========================
-- dim_date
-- =========================
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,   -- YYYYMMDD
    full_date TEXT,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER
);

-- =========================
-- fact_rental_contract
-- =========================
CREATE TABLE fact_rental_contract (
    contract_id TEXT,
    line_number INTEGER,
    contract_type_key INTEGER,
    property_key INTEGER,
    location_key INTEGER,
    tenant_key INTEGER,
    start_date_key INTEGER,
    end_date_key INTEGER,
    contract_amount REAL,
    annual_amount REAL,
    no_of_prop INTEGER,
    is_free_hold INTEGER,
    FOREIGN KEY (contract_type_key) REFERENCES dim_contract_type(contract_type_key),
    FOREIGN KEY (property_key) REFERENCES dim_property(property_key),
    FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
    FOREIGN KEY (tenant_key) REFERENCES dim_tenant(tenant_key),
    FOREIGN KEY (start_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (end_date_key) REFERENCES dim_date(date_key)
);


