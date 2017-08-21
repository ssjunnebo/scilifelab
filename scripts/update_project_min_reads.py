import couchdb
import os

from genologics_sql.queries import get_last_modified_projectids
from genologics.entities import *
from genologics.lims import Lims, 
from genologics.config import BASEURI,USERNAME,PASSWORD

from LIMS2DB.utils import setupServer

import argparse


def main(args):
    lims_db = get_session()
    lims = Lims(BASEURI,USERNAME,PASSWORD)
    couch = setupServer(args.conf)
    db = couch["expected_yields"]

    postgres_string="{} hours".format(args.hours)
    project_ids=get_last_modified_projectids(db_session, postgres_string)

    
    for project in [Project(lims, id=x) for x in project_ids]:
        samples_nb = lims.get_samples_number(project_name=project.name)
        lanes_ordered = project.udf['Sequence units ordered (lanes)']
        key = parse_sequencing_platform(project.udf['Sequencing platform'])
        for row in db.view("min_yield"):
            if row.key==key:
                project.udf['Reads Min']=row.value
                project.put()
            
def parse_sequencing_platform(seq_plat):
    if seq_plat in ["HiseqX", "HiSeqX", "Hiseq X", "HiSeq X"]:
        return ["HiseqX", None, None]

    elif "2500" in seq_plat:
        ar = seq_plat.split(" ")
        return [ar[0], ar[4], "{} {}".format(ar[2], ar[3])]
    elif "MiSeq" in seq_plat:
        ar = seq_plat.split(" ")
        return [ar[0], ar[1], None]





if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--hours',type=int, default=2,
                        help='Amount of hours to check for. Default=2')
    parser.add_argument('--conf',default=os.path.join(os.environ['HOME'],'opt/config/post_process.yaml'),
                        help='Amount of hours to check for. Default=2')
    args = parser.parse_args() 
    main(args)
