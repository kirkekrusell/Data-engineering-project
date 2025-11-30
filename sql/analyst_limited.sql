-- create role
CREATE ROLE IF NOT EXISTS analyst_limited;

-- create user 
CREATE USER IF NOT EXISTS limited_user
IDENTIFIED BY 'limited_strong_password';

-- give the user this role 
GRANT analyst_limited TO limited_user;

-- give the user select rights only on required columns.
GRANT SELECT (company_id, company_name, vat_code, initial_registration_date, legal_form, legal_form_subtype, valid_from, valid_to)
ON TABLE dim_company
TO analyst_limited;
