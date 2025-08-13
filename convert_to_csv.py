import csv

input_file = 'Papers_with_header.txt'
output_file = 'Papers.csv'

with open(input_file, 'r', encoding='utf-8') as infile, \
     open(output_file, 'w', encoding='utf-8', newline='') as outfile:
    
    reader = csv.reader(infile, delimiter='\t')
    writer = csv.writer(outfile)
    
    for row in reader:
        writer.writerow(row)





