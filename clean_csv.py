import csv

input_path = "data/ettevotja_rekvisiidid__lihtandmed.csv"
output_path = "data/ettevotjad_clean.csv"

with open(input_path, "r", encoding="utf-8") as infile, open(output_path, "w", encoding="utf-8", newline="") as outfile:
    reader = csv.reader(infile, delimiter=";")
    writer = csv.writer(outfile, delimiter=";", quoting=csv.QUOTE_MINIMAL)

    for row in reader:
        cleaned = [cell.replace('"', '').strip() for cell in row]
        if len(cleaned) == 17:
            writer.writerow(cleaned)





