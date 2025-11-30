-- create role
CREATE ROLE IF NOT EXISTS analyst_limited;

-- create user 
CREATE USER IF NOT EXISTS limited_user
IDENTIFIED BY 'limited_strong_password';

-- give the user this role 
GRANT analyst_limited TO limited_user;


-- give the user select rights only on required columns.
GRANT SELECT (registry_code, company_id, company_name, vat_code, initial_registration_date, postal_code, legal_form, legal_form_subtype, valid_from, valid_to)
ON db_demo.dim_company
TO analyst_limited;

GRANT SELECT ON db_demo.dim_company_masked TO analyst_limited;

