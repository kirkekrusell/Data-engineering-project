CREATE DATABASE IF NOT EXISTS db_demo;

CREATE TABLE db_demo.dim_company
(
    registry_code              String,
    company_id                 UInt32,
    company_name               String,
    vat_code                   String,
    initial_registration_date  Date,         
    normalized_address         String,         
    postal_code                String,         
    legal_form                 String,  
    legal_form_subtype         String,
    valid_from                 Date,
    valid_to                   Date          
)
ENGINE = MergeTree
ORDER BY registry_code
SETTINGS enable_block_number_column = 1, enable_block_offset_column=1;

INSERT INTO db_demo.dim_company VALUES
(16752073, 1, 'Agent & Partners OÜ', 'EE101335276', '2023-06-05','Harju maakond, Tallinn, Pirita linnaosa, Regati pst 12', '11911','Osaühing', '', '2025-10-30', '2025-10-31'),
(17301777, 2, 'ESTOREON OÜ', 'EE105330266', '2020-02-16','Ubja küla, Rakvere vald, Lääne-Viru maakond', '44203','Osaühing', '', '2025-10-29', '2050-10-29'),
(80662206, 3, 'Mintyn MTÜ', 'EE601555279', '2025-10-16','Harju maakond, Tallinn, Kesklinna linnaosa, Juhkentali tn 8', '10132', 'Mittetulundusühing', 'tavaline mittetulundusühing', '2025-12-01', '2025-12-30'),
(80584373, 4, 'TEGU KLUBI', 'EE102727757', '2020-08-28','Tartu maakond, Tartu linn, Tartu linn, Riia tn 9-13', '51010', 'Mittetulundusühing', 'tavaline mittetulundusühing', '2020-09-20', '2025-09-20'),
(14414780, 5, 'Puksiir OÜ', 'EE101829481', '2018-01-24','Harju maakond, Maardu linn, Keemikute tn 35', '74111', 'Osaühing', '', '2018-02-24', '2030-02-24');
