import os

import pyliftover

# Convert from 37 to 38
lo = pyliftover.LiftOver('hg19', 'hg38')

# Get the directory where the script is located
script_directory = os.path.dirname(os.path.abspath(__file__))
# Define the input directory based on the script's directory
input_directory = os.path.join(script_directory, "input_files")
# List all files in the directory
input_files = [f for f in os.listdir(input_directory) if f.endswith(".txt")]

# Process each input file
for input_file in input_files:
    # Create a list to store the split data
    split_data = []

    # Open and read the input file
    with open(os.path.join(input_directory, input_file), "r") as file:
        for line in file:
            # Skip the header line
            if line.startswith("MARKERNAME"):
                continue

            # Split the line into columns
            columns = line.strip().split("\t")

            # Extract chromosome and position from the MARKERNAME
            markername = columns[0]
            chromosome, position = markername.split(":")

            # liftOver
            result = lo.convert_coordinate(chromosome, int(position))

            if result:
                split_data.append([chromosome, position, result[0][0], result[0][1]]) # noqa E501
            else:
                split_data.append([chromosome, position, 'not found', 'not found']) # noqa E501

    # Define the output file name with the prefix
    output_file = f"flipover_{input_file}"

    # Write the split data to the output file
    with open(os.path.join(input_directory, output_file), "w") as file:
        # Write the header
        file.write("chromosome_38\tposition_38\tchromosome_37\tposition_37\n")
        for data in split_data:
            file.write(f"{data[0]}\t{data[1]}\t{data[2]}\t{data[3]}\n")
