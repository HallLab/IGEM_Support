import pyliftover

# Convert from 37 to 38
lo = pyliftover.LiftOver('hg19', 'hg38')

# Convert a single position
result = lo.convert_coordinate('chrY', 19568371)
print(result)

# Convert a list of positions
positions = [('chrY', 19568371), ('chr1', 722408)]
results = [lo.convert_coordinate(chrom, pos) for chrom, pos in positions]
print(results)
