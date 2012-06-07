import sys
import gzip
from operator import itemgetter

illumina_idx = {'ATCACG':'index1', 
                'ATCACGA':'index1', 
                'CGATGT':'index2',
                'CGATGTA':'index2',
                'TTAGGC':'index3',
                'TTAGGCA':'index3',
                'TGACCA':'index4',
                'TGACCAA':'index4',
                'ACAGTG':'index5',
                'ACAGTGA':'index5',
                'GCCAAT':'index6',
                'GCCAATA':'index6',
                'CAGATC':'index7',
                'CAGATCA':'index7',
                'ACTTGA':'index8',
                'ACTTGAA':'index8',
                'GATCAG':'index9',
                'GATCAGA':'index9',
                'TAGCTT':'index10',
                'TAGCTTA':'index10',
                'GGCTAC':'index11',
                'GGCTACA':'index11',
                'CTTGTA':'index12',
                'CTTGTAA':'index12',
                'AGTCAA':'index13',
                'AGTCAAA':'index13',
                'AGTTCC':'index14',
                'AGTTCCA':'index14',
                'ATGTCA':'index15',
                'ATGTCAA':'index15',
                'CCGTCC':'index16',
                'CCGTCCA':'index16',
                'GTCCGC':'index18',
                'GTCCGCA':'index18',
                'GTGAAA':'index19',
                'GTGAAAA':'index19',
                'GTGGCC':'index20',
                'GTGGCCA':'index20',
                'GTTTCG':'index21',
                'GTTTCGA':'index21',
                'CGTACG':'index22',
                'CGTACGA':'index22',
                'GAGTGG':'index23',
                'GAGTGGA':'index23',
                'ACTGAT':'index25',
                'ACTGATA':'index25',
                'ATTCCT':'index27',
                'ATTCCTA':'index27',
                }

if len(sys.argv)<4:
    print "python", sys.argv[0], "<fastq file containing barcode sequence> <position where bar code starts> <length of bar code>"
    sys.exit(0)
    
bcodes = {} # collect counts for all observed barcodes

gz = False
if sys.argv[1][-2:] == 'gz': gz = True

cntr = 0
last_was_header = False
pos = int(sys.argv[2])
lgth = int(sys.argv[3])

if gz:
    for line in gzip.open(sys.argv[1]):
        if not last_was_header:
            if line[0]=="@": last_was_header=True
            continue

        bcode = line[pos:(pos+lgth)].strip()
        if bcodes.has_key(bcode): bcodes[bcode] += 1
        else: bcodes[bcode] = 1
        if last_was_header: last_was_header = False

else:
    for line in open(sys.argv[1]):
        if not last_was_header:
            if line[0]=="@": last_was_header=True
            continue

        bcode = line[pos:(pos+lgth)].strip()
        if bcodes.has_key(bcode): bcodes[bcode] += 1
        else: bcodes[bcode] = 1
        if last_was_header: last_was_header = False

for e in sorted(bcodes.items(), key=itemgetter(1)):
    illum = '(no exact match to Illumina)'
    if illumina_idx.has_key(e[0]): illum = illumina_idx[e[0]]
    print e[0] + "\t" + str(e[1]) + "\t" + illum
