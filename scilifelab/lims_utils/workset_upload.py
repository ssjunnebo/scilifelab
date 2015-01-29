#!/usr/bin/env python

import argparse
from genologics.entities import Process
from genologics.lims import *
from genologics.lims_utils import *
from genologics.config import BASEURI, USERNAME, PASSWORD
import process_categories as pc 
from datetime import datetime, timedelta
import statusdb.db as sdb
import multiprocessing as mp
import Queue
import logging
import logging.handlers
from pprint import pprint

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
    if args.ws:
        wsp=Process(lims, id=args.ws)
        lc=LimsCrawler(lims, wsp)
        try:
            ws=Workset(lims,lc, log)
        except NameError:
            log.error("no name found for this workset")
        #pprint(ws.obj)
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
    else:
        yesterday = datetime.today() - timedelta(args.days)
        stryes=yesterday.strftime("%Y-%m-%dT%H:%M:%SZ")
        wsts=lims.get_processes(type=pc.WORKSET.values(),last_modified=stryes)
        masterProcess(args, wsts, lims, log)
    
    
    
class Workset:

    def __init__(self, lims, crawler, log):
        self.log=log
        self.name=set()
        self.lims=lims
        self.obj={}
        #get the identifier
        outs=crawler.starting_proc.all_outputs()
        for out in outs:
            if out.type == "Analyte" and len(out.samples) == 1 :
                try:
                    self.name.add(out.location[0].name)
                except:
                    self.log.warn("no name found for workset {}".format(out.id))

        try:      
            self.obj['name']=self.name.pop()
        except:
            self.log.error("No name found for current workset {}, might be an ongoing step.".format(crawler.starting_proc.id))
            raise NameError
        self.obj['technician']=crawler.starting_proc.technician.initials
        pjs={}
        for p in crawler.projects:
            pjs[p.id]={}
            pjs[p.id]['name']=p.name
            try:
                pjs[p.id]['library']=p.udf['Library construction method']
            except KeyError:
                pjs[p.id]['library']=None
            try:
                pjs[p.id]['application']=p.udf['Application']
            except KeyError:
                pjs[p.id]['application']=None

            pjs[p.id]['samples']={}
            for sample in crawler.samples:
                if sample.project == p:
                    pjs[p.id]['samples'][sample.name]={}
                    pjs[p.id]['samples'][sample.name]['library']={}
                    pjs[p.id]['samples'][sample.name]['sequencing']={}
                    try:
                        pjs[p.id]['samples'][sample.name]['customer_name']=sample.udf['Customer Name']
                    except KeyError:
                        pjs[p.id]['samples'][sample.name]['customer_name']= None


                    pjs[p.id]['samples'][sample.name]['rec_ctrl']= {}
                    for i in crawler.starting_proc.all_inputs():
                        if sample in i.samples:
                            pjs[p.id]['samples'][sample.name]['rec_ctrl']['status']=i.qc_flag
                       
                    for output in crawler.starting_proc.all_outputs():
                        if output.type == "Analyte" and sample in output.samples:
                            pjs[p.id]['samples'][sample.name]['location']=output.location[1]

                    

                    for lib in sorted(crawler.libaggre, key=lambda l:l.date_run, reverse=True):
                        for inp in lib.all_inputs():
                            if sample in inp.samples :
                                onelib={}
                                onelib['status']=inp.qc_flag
                                onelib['art']=inp.id
                                onelib['date']=lib.date_run
                                onelib['name']=lib.protocol_name
                                onelib['id']=lib.id
                                pjs[p.id]['samples'][sample.name]['library'][lib.id]=onelib
                                if 'library_status' not in  pjs[p.id]['samples'][sample.name]:
                                    pjs[p.id]['samples'][sample.name]['library_status']=inp.qc_flag




                    for seq in sorted(crawler.seq, key=lambda s:s.date_run, reverse=True):
                        for inp in seq.all_inputs():
                            if sample in inp.samples :
                                pjs[p.id]['samples'][sample.name]['sequencing'][seq.id]={}
                                pjs[p.id]['samples'][sample.name]['sequencing'][seq.id]['status']=inp.qc_flag
                                pjs[p.id]['samples'][sample.name]['sequencing'][seq.id]['date']=seq.date_run
                                if 'sequencing_status' not in  pjs[p.id]['samples'][sample.name]:
                                    pjs[p.id]['samples'][sample.name]['sequencing_status']=inp.qc_flag

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


def processWSUL(options, queue, logqueue):
    mycouch=sdb.Couch()
    mycouch.set_db("worksets")
    mycouch.connect()
    view = mycouch.db.view('worksets/name')
    mylims = Lims(BASEURI, USERNAME, PASSWORD)
    work=True
    procName=mp.current_process().name
    proclog=logging.getLogger(procName)
    proclog.setLevel(level=logging.INFO)
    mfh = QueueHandler(logqueue)
    mft = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    mfh.setFormatter(mft)
    proclog.addHandler(mfh)

    while work:
        #grabs project from queue
        try:
            ws_id = queue.get(block=True, timeout=3)
        except Queue.Empty:
            work=False
            proclog.info("exiting gracefully")
            break
        else:
            wsp=Process(mylims, id=ws_id)
            lc=LimsCrawler(mylims, wsp)
            try:
                ws=Workset(mylims,lc, proclog)
            except NameError:
                continue

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
                    proclog.info("updating {0}".format(ws.obj['name']))
            elif len(view[ws.obj['name']].rows) == 0:
                #it is a new doc, upload it
                mycouch.save(ws.obj) 
                proclog.info("saving {0}".format(ws.obj['name']))
            else:
                proclog.warn("more than one row with name {0} found".format(ws.obj['name']))
            #signals to queue job is done
            queue.task_done()

def masterProcess(options,wslist, mainlims, logger):
    worksetQueue=mp.JoinableQueue()
    logQueue=mp.Queue()
    childs=[]
    procs_nb=1;
    #Initial step : order worksets by date:
    logger.info("ordering the workset list")
    orderedwslist=sorted(wslist, key=lambda x:x.date_run)
    logger.info("done ordering the workset list")
    if len(wslist) < options.procs:
        procs_nb=len(wslist)
    else:
        procs_nb=options.procs

    #spawn a pool of processes, and pass them queue instance 
    for i in range(procs_nb):
        p = mp.Process(target=processWSUL, args=(options,worksetQueue, logQueue))
        p.start()
        childs.append(p)
    #populate queue with data   
    for ws in orderedwslist:
        worksetQueue.put(ws.id)

    #wait on the queue until everything has been processed     
    notDone=True
    while notDone:
        try:
            log=logQueue.get(False)
            logger.handle(log)
        except Queue.Empty:
            if not stillRunning(childs):
                notDone=False
                break

def stillRunning(processList):
    ret=False
    for p in processList:
        if p.is_alive():
            ret=True

    return ret

class QueueHandler(logging.Handler):
    """
    This handler sends events to a queue. Typically, it would be used together
    with a multiprocessing Queue to centralise logging to file in one process
    (in a multi-process application), so as to avoid file write contention
    between processes.

    This code is new in Python 3.2, but this class can be copy pasted into
    user code for use with earlier Python versions.
    """

    def __init__(self, queue):
        """
        Initialise an instance, using the passed queue.
        """
        logging.Handler.__init__(self)
        self.queue = queue

    def enqueue(self, record):
        """
        Enqueue a record.

        The base implementation uses put_nowait. You may want to override
        this method if you want to use blocking, timeouts or custom queue
        implementations.
        """
        self.queue.put_nowait(record)

    def prepare(self, record):
        """
        Prepares a record for queuing. The object returned by this method is
        enqueued.

        The base implementation formats the record to merge the message
        and arguments, and removes unpickleable items from the record
        in-place.

        You might want to override this method if you want to convert
        the record to a dict or JSON string, or send a modified copy
        of the record while leaving the original intact.
        """
        # The format operation gets traceback text into record.exc_text
        # (if there's exception data), and also puts the message into
        # record.message. We can then use this to replace the original
        # msg + args, as these might be unpickleable. We also zap the
        # exc_info attribute, as it's no longer needed and, if not None,
        # will typically not be pickleable.
        self.format(record)
        record.msg = record.message
        record.args = None
        record.exc_info = None
        return record

    def emit(self, record):
        """
        Emit a record.

        Writes the LogRecord to the queue, preparing it for pickling first.
        """
        try:
            self.enqueue(self.prepare(record))
        except Exception:
            self.handleError(record)
                  



if __name__ == '__main__':
    usage = "Usage:       python workset_upload.py [options]"
    parser = argparse.ArgumentParser(description=usage)

    parser.add_argument("-d", "--days", dest="days", type=int, default=90,  
    help = "number of days to look back for worksets")

    parser.add_argument("-p", "--procs", dest="procs", type=int, default=8 ,  
    help = "number of processes to spawn")

    parser.add_argument("-w", "--workset", dest="ws", default=None,
    help = "tries to work on the given ws")

    parser.add_argument("-c", "--conf", dest="conf", 
    default=os.path.join(os.environ['HOME'],'opt/config/post_process.yaml'), 
    help = "Config file.  Default: ~/opt/config/post_process.yaml")

    parser.add_argument("-l", "--log", dest="logfile", 
    default=os.path.join(os.environ['HOME'],'workset_upload.log'), 
    help = "log file.  Default: ~/workset_upload.log")
    args = parser.parse_args()

    main(args)
