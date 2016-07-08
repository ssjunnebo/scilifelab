"""Pm deliver module"""
import os
import re
import grp
import stat
import shutil
import itertools
import glob
import json
import datetime

from cement.core import controller
from collections import defaultdict
from scilifelab.pm.core.controller import AbstractBaseController, AbstractExtendedBaseController
from scilifelab.report import sequencing_success
from scilifelab.report.rl import *
from scilifelab.report.qc import application_qc, fastq_screen, QC_CUTOFF
from scilifelab.bcbio.run import find_samples
from scilifelab.report.delivery_notes import sample_status_note, project_status_note, data_delivery_note
from scilifelab.report.survey import initiate_survey, closed_projects
from scilifelab.report.best_practice import best_practice_note, SEQCAP_KITS
from scilifelab.db.statusdb import SampleRunMetricsConnection, ProjectSummaryConnection, FlowcellRunMetricsConnection, get_scilife_to_customer_name, X_FlowcellRunMetricsConnection
from scilifelab.utils.misc import query_yes_no, filtered_walk, md5sum
from scilifelab.report.gdocs_report import upload_to_gdocs
from scilifelab.utils.timestamp import utc_time
from ConfigParser import NoSectionError, NoOptionError

BCBIO_EXCLUDE_DIRS = ['realign-split', 'variants-split', 'tmp', 'tx', 'fastqc', 'fastq_screen', 'alignments', 'nophix']

## Main delivery controller
class DeliveryController(AbstractBaseController):
    """
    Functionality for deliveries
    """
    class Meta:
        label = 'deliver'
        description = 'Deliver data'
        root_path = None
        path_id = None
        project_root = None
        arguments = [
            (['project'], dict(help="Project name, formatted as 'J.Doe_00_00'", default=None, nargs="?")),
            (['uppmax_project'], dict(help="Uppmax project.", default=None, nargs="?")),
            (['--flowcell'], dict(help="Flowcell id, formatted as YYMMDD_AA000AAXX.", default=None, action="store")),
            (['-i', '--interactive'], dict(help="Interactively select samples to be delivered", default=False, action="store_true")),
            (['-a', '--deliver_all_fcs'], dict(help="rsync samples from all flow cells", default=False, action="store_true")),
            (['-S', '--sample'], dict(help="Project sample id. If sample is a file, read file and use sample names within it. Sample names can also be given as full paths to bcbb-config.yaml configuration file.", action="store", default=None, type=str)),
            (['--no_bam'], dict(help="Don't include bam files in delivery", action="store_true", default=False)),
            (['--no_vcf'], dict(help="Don't include vcf files in delivery", action="store_true", default=False)),
            (['--bam_file_type'], dict(help="bam file type to deliver. Default sort-dup-gatkrecal-realign", action="store", default="sort-dup-gatkrecal-realign")),
            (['-z', '--size'], dict(help="Estimate size of delivery.", action="store_true", default=False)),
            (['-r', '--root'], dict(help="Root directrory to be used instead of pre-defined from config file", action="store", default=None, type=str)),
            (['--statusdb_project_name'], dict(help="Project name in statusdb.", action="store", default=None)),
            (['--group'], dict(help="After raw data delivery, transfer group ownership of the delivered files to this group", action="store", default=None)),
            (['--outdir'], dict(help="Deliver to this (sub)directory instead. Added for cases where the delivery directory already exists and there is no write permission.", action="store", default=None)),
            ]

    def _setup(self, base_app):
        super(DeliveryController, self)._setup(base_app)
        group = base_app.args.add_argument_group('delivery', 'Options affecting data delivery.')
        group.add_argument('--move', help="Transfer file with move", default=False, action="store_true")
        group.add_argument('--copy', help="Transfer file with copy", default=False, action="store_true")
        group.add_argument('--link', help="Create directory structure at target, but symlink files", default=False, action="store_true")
        group.add_argument('--rsync', help="Transfer file with rsync (default)", default=True, action="store_true")
        group.add_argument('--intermediate', help="Work on intermediate data", default=False, action="store_true")
        group.add_argument('--data', help="Work on data folder", default=False, action="store_true")

    def _process_args(self):
        # NB: duplicate of project.ProjectController._process_args
        # setup project search space
        self._meta.project_root = self.pargs.root if self.pargs.root else self.app.config.get("project", "root")
        # Set root path for parent class
        self._meta.root_path = self._meta.project_root
        assert os.path.exists(self._meta.project_root), "No such directory {}; check your project config".format(self._meta.project_root)
        if self.pargs.project:
            self._meta.path_id = self.pargs.project
            # Add intermediate or data
            if self.app.pargs.intermediate:
                if os.path.exists(os.path.join(self._meta.project_root, self._meta.path_id, "nobackup")):
                    self._meta.path_id = os.path.join(self._meta.path_id, "nobackup", "intermediate")
                else:
                    self._meta.path_id = os.path.join(self._meta.path_id, "intermediate")
            if self.app.pargs.data and not self.app.pargs.intermediate:
                if os.path.exists(os.path.join(self._meta.project_root, self._meta.path_id, "nobackup")):
                    self._meta.path_id = os.path.join(self._meta.path_id, "nobackup", "data")
                else:
                    self._meta.path_id = os.path.join(self._meta.path_id, "data")
        # Setup transfer options
        if self.pargs.move:
            self.pargs.rsync = False
        elif self.pargs.copy:
            self.pargs.rsync = False
        super(DeliveryController, self)._process_args()

    @controller.expose(hide=True)
    def default(self):
        print self._help_text

    @controller.expose(help="Deliver raw data")
    def raw_data(self):
        if not self._check_pargs(["project"]):
            return

        # if necessary, reformat flowcell identifier
        if self.pargs.flowcell:
            self.pargs.flowcell = self.pargs.flowcell.split("_")[-1]

        # get the uid and gid to use for destination files
        uid = os.getuid()
        gid = os.getgid()
        if self.pargs.group is not None and len(self.pargs.group) > 0:
            gid = grp.getgrnam(group).gr_gid

        self.log.debug("Connecting to project database")
        p_con = ProjectSummaryConnection(**vars(self.pargs))
        assert p_con, "Could not get connection to project database"
        self.log.debug("Connecting to flowcell database")
        f_con = FlowcellRunMetricsConnection(**vars(self.pargs))
        assert f_con, "Could not get connection to flowcell database"
        self.log.debug("Connecting to x_flowcell database")
        x_con = X_FlowcellRunMetricsConnection(**vars(self.pargs))
        assert x_con, "Could not get connection to x_flowcell database"
        
        # Fetch the Uppnex project to deliver to
        if not self.pargs.uppmax_project:
            self.pargs.uppmax_project = p_con.get_entry(self.pargs.project, "uppnex_id")
            if not self.pargs.uppmax_project:
                self.log.error("Uppmax project was not specified and could not be fetched from project database")
                return
        
        # Find uncompressed fastq
        uncompressed = self._find_uncompressed_fastq_files(proj_base_dir=proj_base_dir, sample=self.pargs.sample, flowcell=self.pargs.flowcell)
        if len(uncompressed) > 0:
            self.log.error("There are uncompressed fastq file for project, kindly check all files are compressed properly before delivery")
            return

        # Setup paths and verify parameters
        self._meta.production_root = self.pargs.root if self.pargs.root else self.app.config.get("production", "root")
        self._meta.root_path = self._meta.production_root
        proj_base_dir = os.path.join(self._meta.root_path, self.pargs.project)
        assert os.path.exists(self._meta.production_root), "No such directory {}; check your production config".format(self._meta.production_root)
        assert os.path.exists(proj_base_dir), "No project {} in production path {}".format(self.pargs.project,self._meta.root_path)

        try:
            self._meta.uppnex_project_root = self.app.config.get("deliver", "uppnex_project_root")
        except Exception as e:
            self.log.warn("{}, will use '/proj' as uppnext_project_root".format(e))
            self._meta.uppnex_project_root = '/proj'

        try:
            self._meta.uppnex_delivery_dir = self.app.config.get("deliver", "uppnex_project_delivery_path")
        except Exception as e:
            self.log.warn("{}, will use 'INBOX' as uppnext_project_delivery_path".format(e))
            self._meta.uppnex_delivery_dir = 'INBOX'

        destination_root = os.path.join(self._meta.uppnex_project_root,self.pargs.uppmax_project,self._meta.uppnex_delivery_dir)
        assert os.path.exists(destination_root), "Delivery destination folder {} does not exist".format(destination_root)
        destination_root = os.path.join(destination_root,self.pargs.project)
        
        # Extract the list of samples and runs associated with the project and sort them
        samples = self.samples_to_copy(pid = p_con.get_entry(self.pargs.project, "project_id"),
                                       fc_dict = {'HiSeq2500':fcon.proj_list, 'HiSeqX':xcon.proj_list},
                                       sample = self.pargs.sample,
                                       flowcell = self.pargs.flowcell,
                                       proj_base_dir = proj_base_dir,
                                       destination_root = destination_root)

        # If interactively select, build a list of samples to skip
        if self.pargs.interactive:
            to_process = {}
            for sample in samples:
                if query_yes_no("Deliver sample {} ?".format(sample), default="no"):
                    to_process[sample] = samples[sample]
            samples = to_process

        if self.pargs.sample:
            sample = samples.get(self.pargs.sample)
                    if not sample:
                        self.log.error("There is no such sample {} for project {}".format(self.pargs.sample, self.pargs.project))
                        return
            samples = {self.pargs.sample: sample}

        self.log.info("Will deliver data for {} samples from project {} to {}".format(len(samples),self.pargs.project,destination_root))
        if not query_yes_no("Continue?"):
            return

        # Make sure that transfer will be with rsync
        if not self.pargs.rsync:
            self.log.warn("Files must be transferred using rsync")
            if not query_yes_no("Do you wish to continue delivering using rsync?", default="yes"):
                return
            self.pargs.rsync = True

        # Process each sample
        for sample, flowcells in samples.iteritems():
            for fc, files in flowcells.iteritems():
                self.log.info("Processing sample {} and flowcell {}".format(sample, fc))

                # transfer files
                self.log.debug("Transferring {} fastq files".format(len(files['src'])))
                self._transfer_files(sources=files['src'], targets=files['dst'])

                passed = True
                if self.pargs.link or self.pargs.dry_run:
                    passed = False
                else:
                    # calculate md5sums on the source side and write it on the destination
                    md5 = []
                    for s,d in zip(files['src'], files['dst']):
                        m = md5sum(s)
                        mfile = "{}.md5".format(d)
                        md5.append([m,mfile,s])
                        self.log.debug("md5sum for source file {}: {}".format(s,m))

                    # write the md5sum to a file at the destination and verify the transfer
                    for m, mfile, srcpath in md5:
                        dstfile = os.path.splitext(mfile)[0]
                        self.log.debug("Writing md5sum to file {}".format(mfile))
                        self.app.cmd.write(mfile,"{}  {}".format(m,os.path.basename(dstfile)),True)
                        self.log.debug("Verifying md5sum for file {}".format(dstfile))
                        dm = md5sum(dstfile)
                        self.log.debug("md5sum for destination file {}: {}".format(dstfile,dm))
                        if m != dm:
                            self.log.warn("md5sum verification FAILED for {}. Source: {}, Target: {}".format(dstfile,m,dm))
                            self.log.warn("Improperly transferred file {} is removed from destination, please retry transfer of this file".format(dstfile))
                            self.app.cmd.safe_unlink(dstfile)
                            self.app.cmd.safe_unlink(mfile)
                            passed = False
                            continue

                        # Modify the permissions to ug+rw
                        for f in [dstfile, mfile]:
                            self.app.cmd.chmod(f,stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP)

                # touch the flag to trigger uppmax inbox permission fix
                self.app.cmd.safe_touchfile(os.path.join("/sw","uppmax","var","inboxfix","schedule",self.pargs.uppmax_project))

                # log the transfer to statusdb if verification passed
                if passed:
                    self.log.info("Logging delivery to StatusDB document {}".format(id))
                    data = {'raw_data_delivery': {'timestamp': utc_time(),
                                                  'files': {os.path.splitext((os.path.basename(srcpath))[0]:{'md5': m,
                                                                                'path': os.path.splitext(mfile)[0],
                                                                                'size_in_bytes': self._getsize(os.path.splitext(mfile)[0]),
                                                                                'source_location': srcpath} for m, mfile, srcpath in md5},
                                                  }
                            }
                    jsonstr = json.dumps(data)
                    jsonfile = os.path.join(proj_base_dir, sample, fc, "{}_{}_raw_data_delivery.json".format(sample, fc))
                    self.log.debug("Writing delivery to json file {}".format(jsonfile))
                    self.app.cmd.write(jsonfile, data=jsonstr, overwrite=True)
                    self.log.debug("Saving delivery in StatusDB document {}".format(id))
                    if self.proj_flowcells[fc]['type'] == 'HiSeqX':
                        fc_con = xcon
                    else:
                        fc_con = fcon
                    fb_obj = fc_con.get_entry(fc)
                    fb_obj['raw_data_delivery'] = fb_obj.get('raw_data_delivery', {}).update(data)
                    self._save(fc_con,fc_obj)
                    self.log.debug(jsonstr)

    def _getsize(self, file):
        """Wrapper around getsize
        """
        size = -1
        try:
            size = os.path.getsize(file)
        except OSError:
            pass
        return size

    def _save(self, con, obj):
        """Dry-run aware wrapper around statusdb save
        """
        def runpipe():
            con.save(obj)
        return self.app.cmd.dry("storing object {} in {}".format(obj.get('_id'),con.db), runpipe)

    def _find_uncompressed_fastq_files(self, proj_base_dir, sample=None, flowcell=None):
        """Finds samples with uncompressed fastq files in project/fc_id/sample_id directories
        Returns a list of uncompressed files
        """
                
        path = proj_base_dir
        path = os.path.join(path, sample if sample else '*')
        path = os.path.join(path, flowcell if flowcell else '*')
        path = os.path.join(path, '*.fastq')
        files = glob.glob(path)
        return files

    def samples_to_copy(self, pid, fc_dict, sample=None, flowcell=None, proj_base_dir, destination_root):
        """Go through FC database and collect samples have been sequenced for a project
        """
        proj_open_date = p_con.get_entry(self.pargs.project, "open_date")
        self.proj_flowcells = {}
        sam_sequenced = defaultdict(dict)
        # collect flowcells sequenced for this project
        for fc_type, proj_list in fc_dict.iteritems():
            sort_fcs = sorted(proj_list.keys(), key=lambda k: datetime.strptime(k.split('_')[0], "%y%m%d"), reverse=True)
            for fc in sort_fcs:
                fc_date, fc_name = fc.split('_')
                if datetime.strptime(fc_date,'%y%m%d') < proj_open_date:
                    break
                if pid in proj_list[fc]:
                    if flowcell and flowcell != fc:
                        continue
                    self.proj_flowcells[fc] = {'name':fc, 'type':fc_type})
        
        if not self.proj_flowcells:
            self.log.error("Could not find any sequenced FC for project {}".format(self.pargs.project))
            return
        
        # go through collected FCs and create source and target list for rsync
        for fc in self.proj_flowcells.values():
            fc_run = fc['name']
            fc_fq_files = glob.glob(os.path.join(proj_base_dir, "{}*".format(pid), fc_run, "*.fastq.gz"))
            if not fc_fq_files:
                self.log.error("FC {} was sequenced for project {}, but could not find any fastq files in {}".format(fc_run, self.pargs.project, proj_base_dir))
                continue
            for src_fq in fc_fq_files:
                samp = os.path.basename(os.path.dirname(os.path.dirname(src_fq)))
                dst_fq = src_fq.replace(proj_base_dir, destination_root)
                if fc_run not in sam_sequenced[samp]:
                    sam_sequenced[samp][fc_run] = defaultdict(list)
                sam_sequenced[samp][fc_run]['src'].append(src_fq)
                sam_sequenced[samp][fc_run]['dst'].append(dst_fq)
        
        return sam_sequenced
    
    @controller.expose(help="Deliver best practice results")
    def best_practice(self):
        if not self._check_pargs(["project", "uppmax_project"]):
            return
        project_path = os.path.normpath(os.path.join("/proj", self.pargs.uppmax_project))
        if not os.path.exists(project_path):
            self.log.warn("No such project {}; skipping".format(self.pargs.uppmax_project))
            return
        if self.pargs.outdir:
            outpath = os.path.join(project_path, "INBOX", self.pargs.outdir)
        else:
            outpath = os.path.join(project_path, "INBOX", self.pargs.statusdb_project_name) if self.pargs.statusdb_project_name else os.path.join(project_path, "INBOX", self.pargs.project)
        if not query_yes_no("Going to deliver data to {}; continue?".format(outpath)):
            return
        if not os.path.exists(outpath):
            self.app.cmd.safe_makedir(outpath)
        kw = vars(self.pargs)
        basedir = os.path.abspath(os.path.join(self._meta.root_path, self._meta.path_id))
        flist = find_samples(basedir, **vars(self.pargs))
        if self.pargs.flowcell:
            flist = [ fl for fl in flist if os.path.basename(os.path.dirname(fl)) == self.pargs.flowcell ]
        if not len(flist) > 0:
            self.log.info("No samples/sample configuration files found")
            return
        def filter_fn(f):
            if not pattern:
                return
            return re.search(pattern, f) != None
        # Setup pattern
        plist = [".*.metrics$"]
        if not self.pargs.no_bam:
            plist.append(".*-{}.bam$".format(self.pargs.bam_file_type))
            plist.append(".*-{}.bam.bai$".format(self.pargs.bam_file_type))
        if not self.pargs.no_vcf:
            plist.append(".*.vcf$")
            plist.append(".*.vcf.gz$")
            plist.append(".*.tbi$")
            plist.append(".*.tsv$")
        pattern = "|".join(plist)
        size = 0
        for f in flist:
            path = os.path.dirname(f)
            sources = filtered_walk(path, filter_fn=filter_fn, exclude_dirs=BCBIO_EXCLUDE_DIRS)
            targets = [src.replace(basedir, outpath) for src in sources]
            self._transfer_files(sources, targets)
            if self.pargs.size:
                statinfo = [os.stat(src).st_size for src in sources]
                size = size + sum(statinfo)
        if self.pargs.size:
            self.app._output_data['stderr'].write("\n********************************\nEstimated delivery size: {:.1f}G\n********************************".format(size/1e9))


    def _transfer_files(self, sources, targets):
        for src, tgt in zip(sources, targets):
            if not os.path.exists(os.path.dirname(tgt)):
                self.app.cmd.safe_makedir(os.path.dirname(tgt))
            self.app.cmd.transfer_file(src, tgt)

## Main delivery controller
class DeliveryReportController(AbstractBaseController):
    """
    Functionality for deliveries
    """
    class Meta:
        label = 'report'
        description = 'Make delivery reports and assess qc'

    def _setup(self, app):
        group = app.args.add_argument_group('Reporting options', 'Options that affect report output')
        group.add_argument('project_name', help="Project name. Standard format is 'J.Doe_00_00'", default=None, nargs="?")
        group.add_argument('flowcell', help="Flowcell id, formatted as AA000AAXX (i.e. without date, machine name, and run number).", default=None, nargs="?")
        group.add_argument('-u', '--uppnex_id', help="Manually insert Uppnex project ID into the report.", default=None, action="store", type=str)
        group.add_argument('-o', '--ordered_million_reads', help="Manually insert the ordered number of read pairs (in millions), either as a string to set all samples, or a JSON string or JSON file to set at a sample level.", default=None, action="store", type=str)
        group.add_argument('-r', '--customer_reference', help="Manually insert customer reference (the customer's name for the project) into reports", default=None, action="store", type=str)
        group.add_argument('--application', help="Set application for qc evaluation. One of '{}'".format(",".join(QC_CUTOFF.keys())), action="store", type=str, default=None)
        group.add_argument('--exclude_sample_ids', help="Exclude project sample ids from report generation. Input is either a string or a JSON file with a key:value mapping, as in '--exclude_sample_ids \"{'PS1':[], 'PS2':['AACCGG']}\"'. The values consist of a list of barcodes; if the list is empty, exclude the entire sample.", action="store", default={})
        group.add_argument('--bc_count', help="Manually set barcode counts in *millions of reads*. Input is either a string or a JSON file with a key:value mapping, as in '--bc_count \"{'Sample1':100, 'Sample2':200}\"'.", action="store", default={})
        group.add_argument('--sample_aliases', help="Provide sample aliases for cases where project summary has multiple names for a sample. Input is either a string or a JSON file with a key:value mapping, for example '--sample_aliases \"{'sample1':['alias1_1', 'alias1_2'], 'sample2':['alias2_1']}\", where the value is a list of aliases. The key will be used as 'base' information, possibly updated by information from the alias entry.", action="store", default={})
        group.add_argument('--project_alias', help="Provide project aliases for cases where project summary has multiple names for a project. Input is a comma-separated list of names enclosed by brackets, for example '--project_alias \"['alias1']\"", action="store", default=None)
        group.add_argument('--phix', help="Provide phix error rate for new illumina flowcells where phix error rate is missing. Input is either a string, or a dictionary/JSON file with a lane:error mapping, for example '--phix \"{1:0.3, 2:0.4}\".", action="store", default=None)
        group.add_argument('--sphinx', help="Generate editable sphinx template. Installs conf.py and Makefile for subsequent report generation.", action="store", default=None, type=float)
        group.add_argument('--project_id', help="Project identifier, formatted as 'P###'.",  action="store", default=None, type=str)
        group.add_argument('--include_all_samples', help="Include all samples in project status report. Default is to only use the latest library prep.",  action="store_true", default=False)
        group.add_argument('--run-id', help="Run id (e.g. 130423_SN9999_0100_BABC123CXX). Required for reporting to Google docs",  action="store", default=None, type=str)
        group.add_argument('--credentials-file', help="Text file containing base64-encoded Google Docs credentials",  action="store", default=None, type=str)
        group.add_argument('--from-date', help="Consider projects closed on or after this date, specified on the form 'YYYY-MM-DD'", default=None, action="store", type=str)
        group.add_argument('--to-date', help="Consider projects closed on or before this date, specified on the form 'YYYY-MM-DD'", default=None, action="store", type=str)
        super(DeliveryReportController, self)._setup(app)

    def _process_args(self):
        pass

    @controller.expose(hide=True)
    def default(self):
        print self._help_text

    @controller.expose(help="Print FastQ screen output for a project/flowcell")
    def fqscreen(self):
        if not self._check_pargs(["project_name"]):
            return
        out_data = fastq_screen(**vars(self.pargs))
        self.app._output_data['stdout'].write(out_data['stdout'].getvalue())
        self.app._output_data['stderr'].write(out_data['stderr'].getvalue())


    @controller.expose(help="Print the SciLife name to customer name conversion table for a project")
    def name_table(self):
        if not self._check_pargs(["project_name"]):
            return
        kw = vars(self.pargs)
        kw.update({"flat_table":True, "samplesdb":self.app.config.get("db", "samples"), "flowcelldb":self.app.config.get("db", "flowcells"), "projectdb":self.app.config.get("db", "projects")})
        out_data = project_status_note(**kw)
        self.app._output_data['stdout'].write(out_data['stdout'].getvalue())
        self.app._output_data['stderr'].write(out_data['stderr'].getvalue())
        self.app._output_data['debug'].write(out_data['debug'].getvalue())

    @controller.expose(help="Report the run statistics to Google Docs")
    def report_to_gdocs(self):
        if self.pargs.project_name is not None:
            self.log.warn("You have specified a project_name, note that this parameter will NOT be used")

        if self.pargs.flowcell is not None:
            self.log.warn("You have specified a flowcell, note that this parameter will NOT be used")

        if not self._check_pargs(["run_id"]):
            self.log.error("You must specify a run id, using the --run-id parameter")
            return

        cfile = self.app.config.get("gdocs","credentials_file")
        if self.pargs.credentials_file is not None:
            cfile = self.pargs.credentials_file

        gdocs_folder = self.app.config.get("gdocs","gdocs_folder")

        out_data = upload_to_gdocs(self.log,os.path.join(self.app.config.get("archive","root"),self.pargs.run_id),
                                   credentials_file=os.path.expanduser(cfile), gdocs_folder=gdocs_folder)

    @controller.expose(help="Print summary QC data for a flowcell/project for application QC control")
    def application_qc(self):
        if not self._check_pargs(["project_name"]):
            return
        out_data = application_qc(**vars(self.pargs))
        self.app._output_data['stdout'].write(out_data['stdout'].getvalue())
        self.app._output_data['stderr'].write(out_data['stderr'].getvalue())


    @controller.expose(help="Make sample status note")
    def sample_status(self):
        if not self._check_pargs(["project_name", "flowcell"]):
            return
        kw = vars(self.pargs)
        kw.update({"samplesdb":self.app.config.get("db", "samples"), "flowcelldb":self.app.config.get("db", "flowcells"), "projectdb":self.app.config.get("db", "projects"), "instrument_config": self.app.config.get("instrument","config")})
        out_data = sample_status_note(**kw)
        self.app._output_data['stdout'].write(out_data['stdout'].getvalue())
        self.app._output_data['stderr'].write(out_data['stderr'].getvalue())
        self.app._output_data['debug'].write(out_data['debug'].getvalue())

    @controller.expose(help="Make project status note")
    def project_status(self):
        if not self._check_pargs(["project_name"]):
            return
        kw = vars(self.pargs)
        kw.update({"samplesdb":self.app.config.get("db", "samples"), "flowcelldb":self.app.config.get("db", "flowcells"), "projectdb":self.app.config.get("db", "projects")})
        out_data = project_status_note(**kw)
        self.app._output_data['stdout'].write(out_data['stdout'].getvalue())
        self.app._output_data['stderr'].write(out_data['stderr'].getvalue())
        self.app._output_data['debug'].write(out_data['debug'].getvalue())

    @controller.expose(help="Make data delivery note")
    def data_delivery(self):
        if not self._check_pargs(["project_name"]):
            return
        kw = vars(self.pargs)
        out_data = data_delivery_note(**kw)
        self.app._output_data['stdout'].write(out_data['stdout'].getvalue())
        self.app._output_data['stderr'].write(out_data['stderr'].getvalue())
        self.app._output_data['debug'].write(out_data['debug'].getvalue())

    @controller.expose(help="Send out a user survey")
    def survey(self):
        if not self._check_pargs(["project_name"]):
            return
        # Send out a user survey if necessary
        self._meta.date_format = "%Y-%m-%d"

        kw = vars(self.pargs)
        for opt in ["smtphost","smtpport","sender","salt"]:
            try:
                kw[opt] = self.app.config.get("email",opt)
            except NoSectionError:
                pass
            except NoOptionError:
                if opt == "salt":
                    self.log.error("You must specify a salt in the 'email' section of the config file for encryption of survey link")
                    return
                pass

        self._meta.salt = kw["salt"]
        initiated = initiate_survey(self,
                                    project=self.pargs.project_name,
                                    **kw)

    @controller.expose(help="List projects that have been closed")
    def closed_projects(self):

        kw = vars(self.pargs)
        # Check that, if specified, the dates are parseable
        for date in ["from_date","to_date"]:
            if kw[date] is not None:
                try:
                    kw[date] = datetime.strptime(kw[date],"%Y-%m-%d")
                except ValueError:
                    self.log.error("Please specify the date using the format 'YYYY-MM-DD'")
                    return

        for project in sorted(closed_projects(self,**kw), key=lambda d: d.get("closed","0000-00-00")):
            print("{}\t{}".format(project["name"],project["closed"]))

    @controller.expose(help="Make best practice reports")
    def best_practice(self):
        self.log.info("Until best practice results are stored in statusDB, best practice reports are generated via the 'pm project bpreport' subcommand. This requires that best practice analyses have been run in the project folder.")
        return

class BestPracticeReportController(AbstractBaseController):
    class Meta:
        label = 'bpreport'
        description = 'Functions for generating best practice reports'

    def _setup(self, app):
        group = app.args.add_argument_group('Best practice report group', 'Options for bpreport')
        group.add_argument('--application', help="Set application for best practice application note." , action="store", type=str, default="seqcap")
        group.add_argument('--capture_kit', help="Set capture for seqcap application note. Either a key, one of '{}', or free text description.".format(",".join(SEQCAP_KITS.keys())), action="store", type=str, default="agilent_v4")
        group.add_argument('--no_statusdb', help="Don't statusdb to convert scilife names to customer names.", action="store_true", default=False)
        group.add_argument('--statusdb_project_name', help="Project name in statusdb.", action="store", default=None)
        super(BestPracticeReportController, self)._setup(app)

    @controller.expose(help="Make best practice reports")
    def bpreport(self):
        if not self._check_pargs(["project"]):
            return
        if not self.pargs.statusdb_project_name:
            self.pargs.statusdb_project_name = self.pargs.project
        kw = vars(self.pargs)
        basedir = os.path.abspath(os.path.join(self.app.controller._meta.root_path, self.app.controller._meta.path_id))
        flist = find_samples(basedir, **vars(self.pargs))
        if not len(flist) > 0:
            self.log.info("No samples/sample configuration files found")
            return
        if self.pargs.no_statusdb:
            sample_name_map = None
        else:
            p_con = ProjectSummaryConnection(dbname=self.app.config.get("db", "projects"), **vars(self.app.pargs))
            s_con = SampleRunMetricsConnection(dbname=self.app.config.get("db", "samples"), **vars(self.app.pargs))
            try:
                sample_name_map = get_scilife_to_customer_name(self.pargs.statusdb_project_name, p_con, s_con, get_barcode_seq=True)
            except ValueError as e:
                self.log.warn(str(e))
                self.log.warn("No such project {} defined in statusdb; try using option --statusdb_project_name".format(self.app.pargs.project))
                sample_name_map = None
        kw.update(project_name=self.pargs.project, flist=flist, basedir=basedir, sample_name_map=sample_name_map)
        out_data = best_practice_note(**kw)
        self.log.info("Wrote report to directory {}; use Makefile to generate pdf report".format(basedir))
        self.app._output_data['stdout'].write(out_data['stdout'].getvalue())
        self.app._output_data['stderr'].write(out_data['stderr'].getvalue())
        self.app._output_data['debug'].write(out_data['debug'].getvalue())


