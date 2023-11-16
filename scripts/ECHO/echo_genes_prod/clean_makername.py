import os

# Get the directory where the script is located
script_directory = os.path.dirname(os.path.abspath(__file__))
# Define the input directory based on the script's directory
input_directory = os.path.join(script_directory, "input_files")
output_directory = os.path.join(script_directory, "output_files")
# List all files in the directory
input_files = [f for f in os.listdir(input_directory) if f.endswith(".txt")]

# Process each input file
for input_file in input_files:
    print("Processing: ", input_file)
    # Create a list to store the split data
    split_data = []

    # Open and read the input file
    with open(os.path.join(input_directory, input_file), "r") as file:
        for line in file:

            # Split the line into columns
            columns = line.strip().split("\t")

            # Extract chromosome and position from the MARKERNAME
            markername = columns[0]
            chromosome, position, allele_1, allele_2 = markername.split(":")
            split_data.append([chromosome, position]) # noqa E501

    # Define the output file name with the prefix
    output_file = f"{input_file}"

    # Write the split data to the output file
    with open(os.path.join(output_directory, output_file), "w") as file:
        # Write the header
        file.write("chromosome\tposition\n")
        for data in split_data:
            file.write(f"{data[0]}\t{data[1]}\n")
