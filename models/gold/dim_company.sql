SELECT
    ariregistri_kood AS registry_code,
    ariregistri_kood AS company_id,
    nimi AS company_name,
    kmkr_nr AS vat_code,
    ettevotja_esmakande_kpv AS initial_registration_date,
    ads_normaliseeritud_taisaadress AS normalized_address,
    indeks_ettevotja_aadressis AS postal_code,
    ettevotja_oiguslik_vorm AS legal_form,
    ettevotja_oigusliku_vormi_alaliik AS legal_form_subtype,
    toDate('2025-01-01') AS valid_from,
    toDate('9999-12-31') AS valid_to
FROM {{ ref('raw_company_data') }}

