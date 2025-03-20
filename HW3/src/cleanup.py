import csv
import sys

input_path = sys.argv[1]
output_file = "data-cleaned.csv"

with open(input_path, "r") as infile, open(output_file, "w") as outfile:
    reader = csv.reader(infile)
    writer = outfile  

    header = next(reader) 
    writer.write(",".join(header) + "\n")

    for line in reader:
        if line == header:
            continue
        
        line[2] = f"{float(line[2]):.2f}"
        formatted_line = f"{line[0]}  {line[1]} {line[2]}"  # Two spaces after first column, one after second

        writer.write(formatted_line + "\n")

print(f"-- file saved as {output_file}")