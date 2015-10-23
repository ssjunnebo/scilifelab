#!/usr/bin/env python
import sys
import os
import optparse
import glob
import yaml
import couchdb
from scilifelab.google import *
import bcbio.pipeline.config_loader as cl

def find_proj_from_view(proj_db, proj_id):
    """Getting proj_id from the project database in statusdb via view"""
    view = proj_db.view('project/project_name')
    for proj in view:
        if proj.key == proj_id:
            return proj.value
    return None

def get_size(n,info,name):
    """fetching insert size from status db"""
    preps = 'FBCDEF'
    prep = ''
    name = "_".join(name.replace('-', '_').replace(' ', '').split("_")[0:2])
    while name[-1] in preps:
        if not name[-1]=='F':
            prep = name[-1] + prep
        name = name[0: -1]
    if prep == '':
            prep = 'A'
    for samp in info['samples']:
        if samp.strip('F')==name.strip('F'):
            try:
                size=info['samples'][samp.strip('F')]['library_prep'][prep]['average_size_bp']
            except:
                key=info['samples'][samp.strip('F')]['library_prep'][prep]['library_validation'].keys()[0]
                size=info['samples'][samp.strip('F')]['library_prep'][prep]['library_validation'][key]['average_size_bp']
        elif name[0]=='P' and name.split('_')[1]==samp:
            print "Best gues for sample found on statusDB corresponding to sample name ",n ," is ",samp
            r = raw_input("If this seems to be correct press y      ")
            if r.upper() == "Y":
                size=info['samples'][samp]['library_prep'][prep]['average_size_bp']
    return int(float(size))

def get_names_from_fastqfiles(fpath,flow_cell):
    if not os.path.exists(fpath):
        sys.exit('no such dir '+fpath)
    flist = glob.glob(str(fpath + '/*'))
    file_info={}
    try_names = 'y'
    ind=-1
    while try_names=='y':
        for f in flist:
            fname = f.split('/')[-1]
            try:
                if (fname.split('.')[-1] == "fastq") | ( fname.split('.')[-2] == "fastq"):
                    lane_run = "_".join(fname.split("_")[0:3])
                    tag = "_".join(fname.split("_")[3:ind])
                    if not file_info.has_key(lane_run):
                        file_info[lane_run] = {}
                    if not file_info[lane_run].has_key(tag):
                        file_info[lane_run][tag] = {}
                if (fname.split('.fastq')[0][-1] == "1"):
                    file_info[lane_run][tag]['R1'] = fname
                else:
                    file_info[lane_run][tag]['R2'] = fname
            except:
                sys.exit('files missing? '+fname)
        print file_info.keys()

        print "Best guess for sample names: "
        for lane in file_info:
            print lane
            print sorted(file_info[lane].keys())
        try_names = raw_input("Press y if the sample names are wrong.")
        if try_names =='y':
            ind=ind-1
            file_info={}
    r = raw_input("Press n to exit")
    if r.upper() == "N": sys.exit(0)
    return file_info

def prepare_lane_run_dir(p,lane):
    an_path = p+'/'+lane
    if not os.path.exists(an_path):
        os.mkdir(an_path)
    return an_path

def frag_len_from_couch(fpath, files, single,samp, info, inner, adapter):
    """trying to get average fragemnt length from couchDB if not single end."""
    if single is True:
        R1 = fpath + '/' + files['R1']
        R2 = ''
        innerdist = ''
        innnerdistflagg = ''
    else:
        R1 = fpath + '/' + files['R1']
        R2 = fpath + '/' + files['R2']
        print R1
        print R2
	if inner is "False":
		innnerdistflagg =""
		innerdist=""
	else:
		innnerdistflagg = '-r'
		if inner=="statusDB" :
        		try:
            			size = get_size(samp,info,samp)
            			print "Average fragment length ", str(size)
			except:
				size = raw_input("Could not find information on statusDB. Enter manualy fragment size including adapters for sample " + samp + ": ")
				pass
			try:
				read_len=info['details']['sequencing_setup'].split('x')[1]
				print "Read length", str(read_len)
			except:
				print "Could not find sequencing setup on statusDB. Use the common setup for paired-end: 125"
				read_len=125
			try:
				innerdist = str(int(size) - int(read_len) - int(read_len) - int(adapter))
			except (NameError,ValueError):
				print "[ERROR]: Wrong/No number specified for the length of adapter, it must be a integer (eg. --adapter 136)."
		else:
			try:
				innerdist=int(inner)
			except:
				print "[Error]: 'inner-dist' must be a integer (eg. --inner-dist 60)"
	return innerdist, innnerdistflagg, R1, R2

def Generat_sbatch_file(an_path,hours ,samp ,mail ,aligner_version,innerdist,refpath,innnerdistflagg,R1,R2,extra_arg, aligner_libtype,fai, qscale):
    """Generating sbatch file for sample"""
    f = open(an_path+"/map_tophat_"+samp+".sh", "w")
    if fai != '':
        make_fai="""genomeCoverageBed -bga -split -ibam accepted_hits_sorted_dupRemoved_"""+samp+""".bam -g """+fai+""" > sample_"""+samp+""".bga"""+ bedGraphToBigWig +""" sample_"""+samp+""".bga"""+fai+""" sample_"""+samp+""".bw"""
    else:
        make_fai=''
    print >>f, """#! /bin/bash -l

#SBATCH -A a2012043
#SBATCH -p core -n 8
#SBATCH -t """+hours+"""
#SBATCH -J tophat_"""+samp+"""
#SBATCH -e tophat_"""+samp+""".err
#SBATCH -o tophat_"""+samp+""".out
#SBATCH --mail-user="""+mail+"""
#SBATCH --mail-type=ALL
"""+extra_arg+"""

module unload bioinfo-tools
module unload bowtie
module unload tophat

module load bioinfo-tools
module load samtools
module load tophat/"""+aligner_version+"""

tophat -o tophat_out_"""+samp+""" """+qscale+""" -p 8 """+aligner_libtype+""" """+innnerdistflagg+""" """+str(innerdist)+""" """+refpath+""" """+R1+""" """+R2+"""
cd tophat_out_"""+samp+"""
mv accepted_hits.bam accepted_hits_"""+samp+""".bam
"""+make_fai
    f.close()

def main(args,phred64,fai,projtag,mail,hours,conffile,fpath,single,stranded,genome,inner,adapter):
    proj_ID = args[0]
    flow_cell = args[1]
    if phred64 == True:
        qscale = '--solexa1.3-quals'
    else:
        qscale = ''
    if not len ( hours.split(':') ) == 3:
        sys.exit("Please specify the time allocation string as hours:minutes:seconds or days-hours:minutes:seconds")
    conf = cl.load_config(conffile)
    port = conf['statusdb']['port']
    username = conf['statusdb']['username']
    password = conf['statusdb']['password']
    URL = username+':'+password+'@'+conf['statusdb']['url']
    extra_arg = "#SBATCH " + conf['sbatch']['extra_arg']
    couch = couchdb.Server("http://" + URL + ':' +str(port))
    proj_db = couch['projects']
    key = find_proj_from_view(proj_db, proj_ID)
    try:
        info = proj_db[key]
    except:
        sys.exit("project "+proj_ID+" not found in statusdb")

    reference_genome = genome if genome else info['reference_genome']
    RNA_analysis_settings = conf['custom_algorithms']['RNA-seq analysis']
    refpath = RNA_analysis_settings[reference_genome]['genomepath']
    aligner_version = RNA_analysis_settings['aligner_version']
    if stranded is True:
        aligner_libtype = RNA_analysis_settings['aligner_libtype']
    else:
        aligner_libtype = ''
    p=os.getcwd()
    if not fpath:
        fpath = p.split('intermediate')[0] + 'data/' + flow_cell
    file_info = get_names_from_fastqfiles(fpath,flow_cell)
    for lane in file_info:
        an_path = prepare_lane_run_dir(p,lane)
        for samp in sorted(file_info[lane]):
		try: 
			innerdist, innnerdistflagg, R1, R2 = frag_len_from_couch(fpath, file_info[lane][samp], single, samp, info, inner, adapter)
			Generat_sbatch_file(an_path,hours ,samp ,mail ,aligner_version,innerdist,refpath,innnerdistflagg,R1,R2,extra_arg, aligner_libtype,fai, qscale)
		except:
			print "--------------------------------------------"
			print "[Error Occured] No sbatch script generated!"

if __name__ == '__main__':
    usage = """make_tophat_sbatch.py <project ID> <Flow cell ID, eg 121113_BD1HG4ACXX>"""
    parser = optparse.OptionParser(usage)

    parser.add_option('-s', '--single', action="store_true", dest="single", default="False",
    help="Run tophat with single end reads.")
    parser.add_option('-c', '--config', action="store", dest="conffile", default=os.path.expanduser("~/opt/config/post_process.yaml"),
    help="Specify config file (post_process.yaml)")
    parser.add_option('-t', '--projtag', action="store", dest="projtag", default="",
    help="Provide a project tag that will be shown in the queuing system to distinguish from other TopHat runs")
    parser.add_option('-p', '--phred64', action="store_true", dest="phred64", default="False",
    help="Use phred64 quality scale")
    parser.add_option('-f', '--fai', action="store", dest="fai", default="",
    help="Provide FASTA index file for generating UCSC bigwig tracks")
    parser.add_option('-m', '--mail', action="store", dest="mail", default=None,
    help="Specify a mailing address for SLURM mail notifications")
    parser.add_option('-a', '--alloc-time', action="store", dest="hours", default="40:00:00",
    help="Time to allocate in SLURM. Please specify as hours:minutes:seconds or days-hours:minutes:seconds")
    parser.add_option('-w', '--fpath', dest="fpath", default=False,
    help="Path to fastq files. If not given, the script assumes the standars structure of the analysis directory'")
    parser.add_option('-r', '--stranded', action="store_true", dest="stranded", default="False",
    help="Run tophat with --librarytype fr-firststranded option for strand-specific RNAseq.")
    parser.add_option('-g', '--genome', action="store", dest="genome", default=None,
    help="Reference genome name (eg. hg19, mm9, rn4, rn5, sacCer2, Zv8, Zv9, Zv10, dm3) Default: fetch from status DB")
    parser.add_option('--inner-dist', action="store", dest="inner", default="False",
    help="useage: --inner-dist statusDB/<int>. Either sepcify an estiamted inner distance between mate pairs for all samples (eg. 100) or fetch the actual value from status DB ('statusDB'). Default: use TopHat default setting for '--mate-inner-dist'")
    parser.add_option('--adapter', action="store", dest="adapter", default="None",
    help="Length of the adapter, mendantory if fetch inner distance from status DB ('--inner-dist statusDB')")

    (opts, args)    = parser.parse_args()
    main(args,opts.phred64,opts.fai,opts.projtag,opts.mail,opts.hours,opts.conffile,opts.fpath,opts.single,opts.stranded,opts.genome,opts.inner,opts.adapter)
