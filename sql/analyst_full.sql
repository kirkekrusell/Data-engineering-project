-- create role
CREATE ROLE IF NOT EXISTS analyst_full;

-- create user 
CREATE USER IF NOT EXISTS full_user
IDENTIFIED BY 'full_strong_password';

-- give the user this role 
GRANT analyst_full TO full_user;

GRANT SELECT ON TABLE dim_company
TO analyst_full;
