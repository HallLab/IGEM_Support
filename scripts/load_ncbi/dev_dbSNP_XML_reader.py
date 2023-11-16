"""

AUTHOR: Andre Rico
DATE: 2022/10/14

Script to transform NCBI/ENTREZ XML Files into CSV. In this case, it was
adapted for dbSNP and selected only SNP and Genes per Chromosome information.

Prcess:
1. Download the NCBI zipped file 
(https://ftp.ncbi.nih.gov/snp/organisms/human_9606/XML/) or
https://ftp.ncbi.nih.gov/snp/organisms/human_9606_b151_GRCh37p13/XML/

2. Unzip the file
3. Map the file in the script by the variable v_path
4. Run the Script
5. The output will be a CSVfile in the same directory as the v_path
6. If exist more files to process, just run the script for each file (chr)

Possible Improvements:
- Download files automatic and decompression
- Option of witch chromosome to process or all
- Process to consolidate all result files into a single or even a database
- Routine to choose output fields fexible

SNP XML Structure
-- this part only includes the rsID block of the file
1. TAG: SnpInfo / ATTR: rsid, observed
    1.1. TAG: SnpLoc / ATTR: genomicAssembly, geneid, geneSymbol, chrom, start,
        locType, rsOrier, contigAllele, contig
    1.2. TAG: Ssinfo / ATTR: ssId, locSnpid, ssOrientToRs
        1.2.1. TAG: ByPop / ATTR: popid, sampleSize
            1.2.1.1. TAG: AlleleFreg / ATTR: allele, freq
            1.2.1.2. TAG: GTypeFreq / ATTR: gtype , freq
    1.3. TAG: GTypeFreq / ATTR: gtype , freq


OUTPUT CSV Structure
rsID, genomicAssembly, geneid, geneSymbol, chrom, start, locType, rsOrier,
    contigAllele, contig

"""

import gzip
import os
import sys
from datetime import datetime
from os.path import dirname, join, realpath
from pathlib import Path
from xml.etree.ElementTree import iterparse

from django.conf import settings

# Add project root to sys.path
try:
    v_root = Path(__file__).parents[3]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("Error: ", e)
    raise

from igem.omics.models import snpgene

v_path = r"/Users/andrerico/downloads/ds_chY.xml.gz"

tm_start = datetime.now()
row_count = 0

# Open the GZIP-compressed file using gzip.open
with gzip.open(v_path, 'rt') as file:  # 'rt' specifies that it's a text file
    # Iterate through the XML file using ElementTree
    for event, elem in iterparse(v_path, events=("end",)):
        if elem.tag == "{http://www.ncbi.nlm.nih.gov/SNP/geno}SnpLoc":
            Snp_id = elem.attrib
            SnpLoc = elem.find("{http://www.ncbi.nlm.nih.gov/SNP/geno}SnpLoc")
            Snp_id["Chrom"] = SnpLoc.attrib.get("chrom")
            Snp_id["ChromStart"] = SnpLoc.attrib.get("start")
            Snp_id["ChromEnd"] = SnpLoc.attrib.get("end")
            Snp_id["SnpLocId"] = SnpLoc.attrib.get("snp_id")

            # Collect more data as needed
            # For example, PhenotypeIds can be collected in a similar way

            # Create an instance of your model and save it to the database
            snpgene_instance = snpgene(
                rsid=Snp_id['Snp_id'],
                observed=Snp_id['PhenotypeIds'],  # You need to replace this with the actual field name  # noqa 501
                genomicassembly="YourAssemblyValue",  # Replace with the actual value  # noqa 501
                chrom=Snp_id['Chrom'],
                start=Snp_id['ChromStart'],
                end=Snp_id['ChromEnd'],
                loctype="YourLocTypeValue",  # Replace with the actual value
                rsorienttochrom="YourOrientChromValue",  # Replace with the actual value  # noqa 501
                contigallele="YourContigAlleleValue",  # Replace with the actual value  # noqa 501
                contig="YourContigValue",  # Replace with the actual value
                geneid="YourGeneIDValue",  # Replace with the actual value
                genesymbol="YourGeneSymbolValue"  # Replace with the actual value  # noqa 501
            )
            snpgene_instance.save()

            row_count += 1

            # Clear the element to free up memory
            elem.clear()

tm_end = datetime.now() - tm_start
print("Processed", row_count, "rows.")
print("Process done in:", tm_end)
