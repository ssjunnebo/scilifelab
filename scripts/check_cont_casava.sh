#!/bin/bash
# Wraper for checking fastq_screen results for a specific project and flowdell
# Assumes casava structure

if [ $# -ne 2 ]; then
  echo "Usage:
        check_cont_casava.sh <flowcell id> <project id>

        Arguments:
        <flowcell id>
                - eg: 120127_BD0H2HACXX
        <project id>
                - eg: M.Muurinen_11_01a"
  exit
fi

fcID=$1
project_id=$2

dir=`pwd`
#cd /bubo/proj/a2010002/production
#cd /bubo/proj/a2010002/nobackup/illumina
cd /gulo/proj_nobackup/a2010002/illumina/
grep -w Human ${project_id}/*/${fcID}/fastq_screen/*.txt | cut -f1,4,8,12 | cut -f2 -d':'
echo ''
grep -w Human_chrX ${project_id}/*/${fcID}/fastq_screen/*.txt | cut -f1,4,8,12 | cut -f2 -d':'
echo ''
grep -w Human_chrY ${project_id}/*/${fcID}/fastq_screen/*.txt | cut -f1,4,8,12 | cut -f2 -d':'
echo ''
grep -w Mouse ${project_id}/*/${fcID}/fastq_screen/*.txt | cut -f1,4,8,12 | cut -f2 -d':'
echo ''
grep -w Ecoli ${project_id}/*/${fcID}/fastq_screen/*.txt | cut -f1,4,8,12 | cut -f2 -d':'
echo ''
grep -w Spruce ${project_id}/*/${fcID}/fastq_screen/*.txt | cut -f1,4,8,12 | cut -f2 -d':'
echo ''
grep -w PhiX ${project_id}/*/${fcID}/fastq_screen/*.txt | cut -f1,4,8,12 | cut -f2 -d':'
cd $dir : phix
