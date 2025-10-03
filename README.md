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
4. Which companies have a single activity notice but cover multiple economic sectors?
5. Average duration of an activity notice before expiration
6. Percentage of companies with all activity notices expired

Datasets
1. Basic Company Data (Business Register)
359,816 rows, 14 columns
Key columns: Name, Register code, Legal form, Status, First entry date, Address, Location codes

2. Notice of Economic Activities (Economic Activity Register)
35,769 rows, 8 columns
Key columns: Number, Entrepreneur name, Register code, Start & End date, Valid, Field of activity

You can create fact and dimension tables with this SQL script

```sql
-- 1. Company Fact Table
-- Grain: 1 row per company (per business registration code)

CREATE TABLE Company_fact_table (
    company_key INT PRIMARY KEY,
    legal_form_key INT NOT NULL,
    status_key INT NOT NULL,
    FOREIGN KEY (legal_form_key) REFERENCES Legal_form_dim(legal_form_key),
    FOREIGN KEY (status_key) REFERENCES Status_dim(status_key)
);

-- 2. MTR Fact Table
-- Grain: 1 row per MTR register code (majandustegevusteate nr)

CREATE TABLE MTR_fact_table (
    mtr_register_code INT PRIMARY KEY,
    company_key INT NOT NULL,
    valid_from DATE,
    valid_to DATE,
    activity_key INT NOT NULL,
    FOREIGN KEY (company_key) REFERENCES Company_dim(company_key),
    FOREIGN KEY (activity_key) REFERENCES Activity_dim(activity_key)
);

-- Company Dimension
-- SCD Type 2: keep history of name/address changes
CREATE TABLE Company_dim (
    company_key INT PRIMARY KEY,
    business_register_code VARCHAR(50) NOT NULL,
    name VARCHAR(255),
    kmkr_nr VARCHAR(50),
    valid_from DATE NOT NULL,
    valid_to DATE,
    normalized_address VARCHAR(255),
    postal_code VARCHAR(20),
    adr_id VARCHAR(50)
);

-- Legal Form Dimension
-- SCD Type 0: static list of legal forms
CREATE TABLE Legal_form_dim (
    legal_form_key INT PRIMARY KEY,
    legal_form VARCHAR(100) NOT NULL,
    legal_form_subtype VARCHAR(100)
);

-- Status Dimension
-- SCD Type 0: static list of statuses
CREATE TABLE Status_dim (
    status_key INT PRIMARY KEY,
    company_status VARCHAR(50),
    company_status_text VARCHAR(255)
);

-- Date Dimension
-- SCD Type 0: static calendar
CREATE TABLE Date_dim (
    date_key INT PRIMARY KEY,
    year INT,
    month INT,
    day INT
);

-- Activity Dimension
-- SCD Type 0: static set of activities
CREATE TABLE Activity_dim (
    activity_key INT PRIMARY KEY,
    area_of_activity VARCHAR(255),
    type_of_activity VARCHAR(100),
    extra_info TEXT
);
```
