import pandas as pd

# Read the text file
with open('/users/andrerico/dev/projects/igem/igem_support/IGEM_Support_Data/scripts/kegg/pathway', 'r') as file:
    data = file.read()

    # Split the data into individual records
    records = data.strip().split('///')

    # Process each record and extract the desired information
    results = []
    last_key = ''
    for record in records:
        if record == '':
            continue

        lines = record.strip().split('\n')

        record_data = {}
        for line in lines:
            key, value = line.split(' ', maxsplit=1)
            if key == '':
                record_data[last_key] = record_data[last_key] + ';' + value
            else:
                if last_key != 'REFERENCE':
                    last_key = key
                    record_data[key.strip()] = value.strip()
                elif key != 'REFERENCE':
                    last_key = key
                    record_data[key.strip()] = value.strip()
        if 'ORGANISM' in record_data:
            if record_data['ORGANISM'] == 'Homo sapiens (human) [GN:hsa]':
                results.append(record_data)

# Convert the list of dictionaries into a DataFrame
df = pd.DataFrame(results)

# Save the DataFrame as a CSV file
df.to_csv('/users/andrerico/dev/projects/igem/igem_support/IGEM_Support_Data/scripts/kegg/output.csv', index=False)
