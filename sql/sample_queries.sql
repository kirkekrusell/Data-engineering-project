--- Find the registry codes of companies
SELECT company_name, registry_code
FROM db_demo.dim_company;

SELECT company_name, registry_code_masked
FROM db_demo.dim_company_masked;

--- Group companies by county
SELECT
    splitByChar(',', normalized_address)[1] AS county,
    count(*) AS companies
FROM db_demo.dim_company
GROUP BY county
ORDER BY companies DESC;

SELECT
    splitByChar(',', normalized_address_masked)[1] AS county,
    count(*) AS companies
FROM db_demo.dim_company_masked
GROUP BY county
ORDER BY companies DESC;

--- Find companies with 5-digit postal codes starting with '44'
SELECT registry_code, company_name, postal_code
FROM db_demo.dim_company
WHERE postal_code LIKE '44%';

SELECT registry_code_masked, company_name, postal_code_masked
FROM db_demo.dim_company_masked
WHERE postal_code_masked LIKE '***44%';
