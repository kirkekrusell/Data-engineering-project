-- create role
CREATE ROLE IF NOT EXISTS analyst_limited;

-- create user 
CREATE USER IF NOT EXISTS limited_user
IDENTIFIED BY 'limited_strong_password';

-- give the user this role 
GRANT analyst_limited TO limited_user;

-- give the user select rights only on required columns.
GRANT SELECT (...)
ON ...
TO analyst_limited;

GRANT SELECT (...)
ON ... 
TO analyst_limited;
