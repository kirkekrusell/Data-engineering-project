# Project Overview
This project aims to analyze Estonian companies’ economic activities by combining basic company data from the Business Register with economic activity areas from the MTR (Main Economic Activity Registry). The goal is to identify sector trends, regional differences, and company activity patterns across Estonia.

Stakeholders   
1. Data Analysts
2. Policymakers
3. Investors
4. Banks and Financial Institutions
5. Public and Media

Key Metrics (KPIs)
1. Number of activity notices per company
2. Share of companies with expired activity notices

Business Questions
1. How many companies have multiple activity notices and operate in multiple sectors?
2. How many companies registered their economic activity areas in the same year they were established?
3. How many companies have terminated at least one economic activity notice?
4. Average duration of an activity notice before expiration
5. Percentage of companies with all activity notices expired

Datasets
1. Basic Company Data (Business Register)
359,816 rows, 14 columns
Key columns: Name, Register code, Legal form, Status, First entry date, Address, Location codes

2. Notice of Economic Activities (Economic Activity Register)
35,769 rows, 9 columns
Key columns: Number, Entrepreneur name, Register code, Start & End date, Valid, Field of activity, Type of area of activity and Additional_info 

You can create fact and dimension tables with this SQL script

```sql
-- =====================================================
-- 1. FACT TABLES
-- =====================================================

-- 1.1 Company Fact Table
-- Grain: 1 row per company (per business register code)
CREATE TABLE fact_companies (
    fact_company_id SERIAL PRIMARY KEY,
    dim_companies_id INT NOT NULL,
    dim_legal_form_id INT NOT NULL,
    dim_status_id INT NOT NULL,
    FOREIGN KEY (dim_companies_id) REFERENCES dim_companies(id),
    FOREIGN KEY (dim_legal_form_id) REFERENCES dim_legal(id),
    FOREIGN KEY (dim_status_id) REFERENCES dim_status(id)
);

-- 1.2 MTR Fact Table
-- Grain: 1 row per MTR registry code (economic activity notice)
CREATE TABLE fact_mtr (
    mtr_registry_code VARCHAR(50) PRIMARY KEY,
    dim_companies_id INT NOT NULL,
    valid_from DATE,
    valid_to DATE,
    dim_activity_id INT NOT NULL,
    FOREIGN KEY (dim_companies_id) REFERENCES dim_companies(id),
    FOREIGN KEY (dim_activity_id) REFERENCES dim_activity(id)
);

-- =====================================================
-- 2. DIMENSION TABLES
-- =====================================================

-- 2.1 Company Dimension (SCD Type 2)
CREATE TABLE dim_companies (
    id SERIAL PRIMARY KEY,
    registry_code VARCHAR(20) NOT NULL,
    company_name VARCHAR(255),
    vat_code VARCHAR(20),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    normalized_address VARCHAR(500),
    postal_code VARCHAR(10),
    adr_id VARCHAR(50)
);

-- 2.2 Legal Form Dimension (SCD Type 0)
CREATE TABLE dim_legal (
    id SERIAL PRIMARY KEY,
    legal_form VARCHAR(100) NOT NULL,
    legal_form_subtype VARCHAR(100)
);

-- 2.3 Status Dimension (SCD Type 0)
CREATE TABLE dim_status (
    id SERIAL PRIMARY KEY,
    status_code VARCHAR(50),
    status_name VARCHAR(255)
);

-- 2.4 Date Dimension (SCD Type 0)
CREATE TABLE dim_date (
    id SERIAL PRIMARY KEY,
    year INT CHECK (year BETWEEN 1900 AND 2100),
    month INT CHECK (month BETWEEN 1 AND 12),
    day INT CHECK (day BETWEEN 1 AND 31)
);

-- 2.5 Activity Dimension (SCD Type 0)
CREATE TABLE dim_activity (
    id SERIAL PRIMARY KEY,
    activity_area VARCHAR(255),
    activity_type VARCHAR(100),
    additional_info TEXT
);
```
Demo Queries

Siia võiks lisada need näited, mida me oma andmebaasiga teha saame (Kirke tehtud SQL)
