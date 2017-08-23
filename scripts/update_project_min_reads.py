import couchdb
import argparse
import os
import yaml

from genologics_sql.queries import get_last_modified_projectids
from genologics_sql.utils import get_session
from genologics.entities import *
from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD

from LIMS2DB.utils import setupServer


def main(args):
    lims_db = get_session()
    lims = Lims(BASEURI,USERNAME,PASSWORD)
    with open(args.conf) as cf:
        db_conf = yaml.load(cf)
        couch = setupServer(db_conf)
    db = couch["expected_yields"]
    postgres_string="{} hours".format(args.hours)
    project_ids=get_last_modified_projectids(lims_db, postgres_string)

    for project in [Project(lims, id=x) for x in project_ids]:
        samples_count = 0
        samples = lims.get_samples(projectname=project.name)
        for sample in samples:
            if not("Status (manual)" in sample.udf and sample.udf["Status (manual)"] == "Aborted"):
                samples_count +=1
        lanes_ordered = project.udf['Sequence units ordered (lanes)']
        key = parse_sequencing_platform(project.udf['Sequencing platform'])
        for row in db.view("yields/min_yield"):
            db_key = [x.lower() if x else None for x in row.key]
            if db_key==key:
                try:
                    project.udf['Reads Min'] = float(row.value) * lanes_ordered / samples_count
                    project.put()
                except ZeroDivisionError:
                    pass

def parse_sequencing_platform(seq_plat):
    seq_plat = seq_plat.lower()
    if seq_plat in ["hiseqx", "hiseq x"]:
        return ["hiseqx", None, None]

    elif "2500" in seq_plat:
        ar = seq_plat.split(" ")
        if "rapid" in ar:
            return [ar[0].lower(), None, ar[2].lower()]
        else:
            try:
                return [ar[0], ar[4], "{} {}".format(ar[2].lower(), ar[3].lower())]
            except:
                return[ar[0], None, "{} {}".format(ar[2].lower(), ar[3].lower())]

    elif "miSeq" in seq_plat:
        ar = seq_plat.split(" ")
        return [ar[0], ar[1], None]





if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--hours',type=int, default=2,
                        help='Amount of hours to check for. Default=2')
    parser.add_argument('--conf',default=os.path.join(os.environ['HOME'],'opt/config/post_process.yaml'),
                        help='Amount of hours to check for. Default=2')
    args = parser.parse_args() 
    main(args)
