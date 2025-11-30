--- Find the registry codes of companies
SELECT company_name, registry_code
FROM db_demo.dim_company;

--- Group companies by county
SELECT
    splitByChar(',', normalized_address)[1] AS county,
    count(*) AS companies
FROM db_demo.dim_company
GROUP BY county
ORDER BY companies DESC;

--- Find companies with 5-digit postal codes starting with '44'
SELECT registry_code, company_name, postal_code
FROM db_demo.dim_company
WHERE postal_code LIKE '44%';
