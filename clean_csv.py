import csv
from datetime import datetime

input_path = "data/ariregister_ilma_url.csv"
output_path = "data/ariregister_clean.csv"

def fix_date(date_str):
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except:
        return ""

with open(input_path, "r", encoding="utf-8-sig") as infile:
    with open(output_path, "w", encoding="utf-8", newline="") as outfile:

        reader = csv.reader(infile, delimiter=",")
        writer = csv.writer(outfile, delimiter=";", quoting=csv.QUOTE_NONE, escapechar='\\', lineterminator="\n")

        for i, row in enumerate(reader):
            cleaned = [cell.replace('"', '').replace(',', ' ').replace(';', ' ').strip() for cell in row]

            if i == 0:
                writer.writerow([
                    "nimi",
                    "ariregistri_kood",
                    "kmkr_nr",
                    "ettevotja_esmakande_kpv",
                    "ads_normaliseeritud_taisaadress",
                    "indeks_ettevotja_aadressis",
                    "ettevotja_oiguslik_vorm",
                    "ettevotja_oigusliku_vormi_alaliik"
                ])
            elif len(cleaned) >= 16:
                cleaned[7] = fix_date(cleaned[7])
                writer.writerow([
                    cleaned[0],  # nimi
                    cleaned[1],  # ariregistri_kood
                    cleaned[4],  # kmkr_nr
                    cleaned[7],  # esmakande_kpv
                    cleaned[15], # normaliseeritud_aadress
                    cleaned[12], # indeks
                    cleaned[2],  # oiguslik vorm
                    cleaned[3]   # alaliik
                ])
