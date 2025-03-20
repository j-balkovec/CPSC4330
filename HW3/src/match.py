import csv
import sys

def clean_csv(input_path, output_path="data-cleaned.csv"):
    with open(input_path, "r") as infile, open(output_path, "w") as outfile:
        reader = csv.reader(infile)
        writer = outfile
        
        header = next(reader)
        writer.write(",".join(header) + "\n")

        for line in reader:
            if line == header:  
                continue

            line = [col.replace("\t", " ") for col in line]  
            line[2] = f"{float(line[2]):.2f}"  

            formatted_line = f"{line[0]}  {line[1]} {line[2]}"
            writer.write(formatted_line + "\n")

    print(f"-- file saved as {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python clean_csv.py <input_file>")
        sys.exit(1)

    clean_csv(sys.argv[1])