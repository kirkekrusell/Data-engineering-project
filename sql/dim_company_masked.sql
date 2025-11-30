CREATE OR REPLACE VIEW db_demo.dim_company_masked AS
SELECT

    --- registry_code: keep only last 4 digits
    concat('***', right(registry_code, 4)) AS registry_code_masked,

    company_id,
    company_name,
    vat_code,
    initial_registration_date,

    --- normalized_adress: replace with NULL
    NULL AS normalized_address_masked,
      
    --- postal_code: keep only first 2 digits
    concat('***', left(postal_code, 2)) AS postal_code_masked,

    legal_form,
    legal_form_subtype,
    valid_from,
    valid_to

FROM db_demo.dim_company;
