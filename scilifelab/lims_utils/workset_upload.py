#!/usr/bin/env python

import argparse
from genologics.lims import *
from genologics.lims_utils import *
from genologics.config import BASEURI, USERNAME, PASSWORD
import process_categories as pc 
from datetime import datetime, timedelta
import statusdb.db as sdb

def setupLog(args):
    mainlog = logging.getLogger('worksetlogger')
    mainlog.setLevel(level=logging.INFO)
    mfh = logging.FileHandler(args.logfile)
    mft = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    mfh.setFormatter(mft)
    mainlog.addHandler(mfh)
    return mainlog

def main(args):
    log=setupLog(args)
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    #this will decide how far back we are looking
    yesterday = datetime.today() - timedelta(args.days)
    stryes=yesterday.strftime("%Y-%m-%dT%H:%M:%SZ")
    wsts=lims.get_processes(type=pc.WORKSET.values(),last_modified=stryes)
    #wsts=lims.get_processes(type=pc.WORKSET.values())
    wsts=[Process(lims, id='24-79196')]
    for pr in wsts:
        lc=LimsCrawler(lims, pr)
        ws=Workset(lims,lc)
        mycouch=sdb.Couch()
        mycouch.set_db("worksets")
        mycouch.connect()
        view = mycouch.db.view('worksets/name')

        #If there is already a workset with that name in the DB
        if len(view[ws.obj['name']].rows) == 1:
            remote_doc=view[ws.obj['name']].rows[0].value
            #remove id and rev for comparison
            doc_id=remote_doc.pop('_id')
            doc_rev=remote_doc.pop('_rev')
            if remote_doc != ws.obj:
                #if they are different, though they have the same name, upload the new one
                ws.obj['_id']=doc_id
                ws.obj['_rev']=doc_rev
                mycouch.db[doc_id]=ws.obj 
                log.info("updating {0}".format(ws.obj['name']))
        elif len(view[ws.obj['name']].rows) == 0:
            #it is a new doc, upload it
            mycouch.save(ws.obj) 
            log.info("saving {0}".format(ws.obj['name']))
        else:
            log.warn("more than one row with name {0} found".format(ws.obj['name']))
    
    
    
class Workset:

    def __init__(self, lims, crawler):
        self.name=set()
        self.lims=lims
        self.obj={}
        #get the identifier
        outs=crawler.starting_proc.all_outputs()
        for out in outs:
            if out.type == "Analyte" and len(out.samples) == 1 :
                self.name.add(out.location[0].name)
        self.obj['name']=self.name.pop()
        self.obj['technician']=crawler.starting_proc.technician.initials
        pjs={}
        for p in crawler.projects:
            pjs[p.id]={}
            pjs[p.id]['name']=p.name
            if "library" in p.udf:
                pjs[p.id]['library']=p.udf['Library construction method']
            if "application" in p.udf:
                pjs[p.id]['application']=p.udf['Application']
            pjs[p.id]['samples']={}
            for sample in crawler.samples:
                if sample.project == p:
                    pjs[p.id]['samples'][sample.name]={}
                    pjs[p.id]['samples'][sample.name]['library']={}
                    pjs[p.id]['samples'][sample.name]['sequencing']={}
                    try:
                        pjs[p.id]['samples'][sample.name]['Customer Name']=sample.udf['Customer Name']
                    except KeyError:
                        pjs[p.id]['samples'][sample.name]['Customer Name']= None


                    pjs[p.id]['samples'][sample.name]['rec_ctrl']= {}
                    for i in crawler.inputs:
                        if sample in i.samples:
                            pjs[p.id]['samples'][sample.name]['rec_ctrl']['status']=i.qc_flag
                       
                    for output in crawler.starting_proc.all_outputs():
                        if output.type == "Analyte" and sample in output.samples:
                            pjs[p.id]['samples'][sample.name]['location']=output.location[1]
                    for lib in crawler.libaggre:
                        for inp in lib.all_inputs():
                            if sample in inp.samples :
                                pjs[p.id]['samples'][sample.name]['library'][lib.id]={}
                                pjs[p.id]['samples'][sample.name]['library'][lib.id]['status']=inp.qc_flag
                                pjs[p.id]['samples'][sample.name]['library'][lib.id]['date']=lib.date_run
                                pjs[p.id]['samples'][sample.name]['library'][lib.id]['name']=lib.protocol_name

                    for seq in crawler.seq:
                        for inp in seq.all_inputs():
                            if sample in inp.samples :
                                pjs[p.id]['samples'][sample.name]['sequencing'][seq.id]={}
                                pjs[p.id]['samples'][sample.name]['sequencing'][seq.id]['status']=inp.qc_flag
                                pjs[p.id]['samples'][sample.name]['sequencing'][seq.id]['date']=seq.date_run

        self.obj['projects']=pjs
                    

        


class LimsCrawler:
    
    def __init__(self, lims,starting_proc):
        self.lims=lims
        self.starting_proc=starting_proc
        self.samples=set()
        self.projects=set()
        self.preprepstart=set()
        self.prepstart=set()
        self.prepend=set()
        self.libval=set()
        self.libaggre=set()
        self.seq=set()
        self.demux=set()
        self.inputs=set()
        for i in starting_proc.all_inputs():
            if i.type == "Analyte":
                self.samples.update(i.samples)
                self.inputs.add(i)
        for sample in self.samples:
            if sample.project:
                self.projects.add(sample.project)
        self.crawl(starting_proc)

    def crawl(self,starting_step):
        nextsteps=set()
        for o in starting_step.all_outputs():
            if o.type == "Analyte" and (self.samples.intersection(o.samples)):
                nextsteps.update(self.lims.get_processes(inputartifactlimsid=o.id))
        for step in nextsteps:
            if step.type.name in pc.PREPREPSTART.values():
                self.preprepstart.add(step)
            elif step.type.name in pc.PREPSTART.values():
                self.prepstart.add(step)
            elif step.type.name in pc.PREPEND.values():
                self.prepend.add(step)
            elif step.type.name in pc.LIBVAL.values():
                self.libval.add(step)
            elif step.type.name in pc.AGRLIBVAL.values():
                self.libaggre.add(step)
            elif step.type.name in pc.SEQUENCING.values():
                self.seq.add(step)
            elif step.type.name in pc.DEMULTIPLEX.values():
                self.demux.add(step)

            #if the step has analytes as outputs
            if filter(lambda x : x.type=="Analyte", step.all_outputs()):
                self.crawl(step)






if __name__ == '__main__':
    usage = "Usage:       python workset_upload.py [options]"
    parser = argparse.ArgumentParser(description=usage)

    parser.add_argument("-d", "--days", dest="days", type=int, default=1 ,  
    help = "number of days to look back for worksets")

    parser.add_argument("-c", "--conf", dest="conf", 
    default=os.path.join(os.environ['HOME'],'opt/config/post_process.yaml'), 
    help = "Config file.  Default: ~/opt/config/post_process.yaml")

    parser.add_argument("-l", "--log", dest="logfile", 
    default=os.path.join(os.environ['HOME'],'workset_upload.log'), 
    help = "log file.  Default: ~/workset_upload.log")
    args = parser.parse_args()

    main(args)
