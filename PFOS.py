# add_header.py
input_path = "/home/emcj/data/MAG/PaperFieldsOfStudy.txt"
output_path = "PaperFieldsOfStudy_with_header.txt"
columns = ['PaperId', 'FieldOfStudyId', 'Score']

with open(input_path, "r", encoding="utf-8", errors="ignore") as infile, open(output_path, "w") as outfile:
    outfile.write("\t".join(columns) + "\n")
    for line in infile:
        outfile.write(line)


