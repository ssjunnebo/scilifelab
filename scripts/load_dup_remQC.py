import sys
import os
import json
from argparse import ArgumentParser

def main(stat_file, single, min, proj):
    min_reads_aft_dup_rem = (float(min)/2.0)
    qc_dict = {}
    dict = eval(open('stat.json').read())
    for key, val in dict.items():
        if single:
            M_reads_aft_dup_rem = round(float(
                   val['aft_dup_rem']['mapq >= mapq_cut (unique)'])/1000000.0,2)
        else:
            R1 = val['aft_dup_rem']['Read-1']
            R2 = val['aft_dup_rem']['Read-2']
            M_reads_aft_dup_rem = round((float(R2)+float(R1))/2000000,2)
        if M_reads_aft_dup_rem > min_reads_aft_dup_rem:
            qc_passed = True
        else:
            qc_passed = False
        print "{0}  : {1} : {2} : {3}".format(key, qc_passed ,
                           str(M_reads_aft_dup_rem), str(min_reads_aft_dup_rem))
        qc_dict[key] = {"automated_qc": {
                                "qc_passed": qc_passed,
                                "qc_reason": "M reads after duplicates removed: {0}. Minimal amount after duplicates removed: {1}".format(str(M_reads_aft_dup_rem), min_reads_aft_dup_rem)}}
    f_path = '/proj/a2012043/private/nobackup/app_QC/'
    j = json.dumps(qc_dict)
    f = open(f_path + proj + '.json', 'w')
    print >> f, j
    f.close()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('-f', default = 'stat.json' , dest = 'stat_file',
                        help='stat.json file - output from rna-seq pipe')
    parser.add_argument('-s', default = False, action="store_true", dest = 'single',
                        help=('Single end reads.'))
    parser.add_argument('-m', default = False, dest = 'min', type = float,
                            help=('Min reads (M)'))
    parser.add_argument('-p', default = False, dest = 'proj',
                                 help=('project name'))


    args = parser.parse_args()
    main(args.stat_file, args.single, args.min, args.proj)
