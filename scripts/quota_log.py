"""Generates dictionaries for the load on the quota
using 'uquota'. If a couchdb is specified, the dictionaries will be sent there.
Otherwise prints the dictionaries.
"""
import datetime
import subprocess
import couchdb
import yaml
import argparse
import os
import logging
import sys


DEFAULT_CONFIG = os.path.expanduser("~/.ngi_config/statusdb.yaml")

def disk_quota():
    current_time = datetime.datetime.now()
    try:
        uq = subprocess.Popen(["/sw/uppmax/bin/uquota", "-q"], stdout=subprocess.PIPE)
    except Exception, e:
        logging.error(e.message)
        raise

    output = uq.communicate()[0]
    logging.info("Disk Usage:")
    logging.info(output)

    projects = output.split("\n/proj/")[1:]

    result = {}
    for proj in projects:
        project_dict = {"time": current_time.isoformat()}

        project = proj.strip("\n").split()
        project_dict["project"] = project[0]
        project_dict["usage (GB)"] = project[1]
        project_dict["quota limit (GB)"] = project[2]
        try:
            project_dict["over quota"] = project[3]
        except:
            pass

        result[project[0]] = project_dict
    return result



def cpu_hours():
    current_time = datetime.datetime.now()
    try:
        # script that runs on uppmax
        uq = subprocess.Popen(["/sw/uppmax/bin/projinfo", '-q'], stdout=subprocess.PIPE)
    except Exception, e:
        logging.error(e.message)
        raise

    # output is lines with the format: project_id  cpu_usage  cpu_limit
    output = uq.communicate()[0]

    logging.info("CPU Hours Usage:")
    logging.info(output)

    result = {}
    # parsing output
    for proj in output.strip().split('\n'):
        project_dict = {"time": current_time.isoformat()}

        # split line into a list
        project = proj.split()
        # creating objects
        project_dict["project"] = project[0]
        project_dict["cpu hours"] = project[1]
        project_dict["cpu limit"] = project[2]

        result[project[0]] = project_dict
    return result


def save_results(disk_quota_data, cpu_hours_data, db_config):
    merged_results = disk_quota_data

    # merging 2 dicts into one
    for project in cpu_hours_data.keys():
        if project in disk_quota_data.keys():
            # add keys if project already in the list
            merged_results[project]['cpu hours'] = cpu_hours_data[project]['cpu hours']
            merged_results[project]['cpu limit'] = cpu_hours_data[project]['cpu limit']
        else:
            # add project if not in the list
            merged_results[project] = cpu_hours_data[project]

    logging.info("Connecting to the Database: {url}".format(url=db_config['url']))
    # create db instance
    server = "http://{username}:{password}@{url}:{port}".format(
        url=db_config['url'],
        username=db_config['username'],
        password=db_config['password'],
        port=db_config['port'])
    try:
        couch = couchdb.Server(server)
    except Exception, e:
        logging.error(e.message)
        raise

    db = couch['uppmax']
    logging.info('Connection established')

    # save results in the database
    for project in merged_results.values():
        try:
            project_id, project_rev = db.save(project)
        except Exception, e:
            logging.error(e.message)
            raise
        else:
            logging.info('Project: {id} {name} has been updated'.format(id=project_id, name=project['project']))



if __name__ == "__main__":
    # parse arguments from command line
    parser = argparse.ArgumentParser(description="Formats uquota \
        information as a dict, and sends it to a given CouchDB.")

    parser.add_argument("--config", dest="config", default=DEFAULT_CONFIG, \
        help="Path to the DB config file.")

    parser.add_argument("--log-level", dest="log_level", default="error", help="Log level: debug, info, error, critical")
    parser.add_argument("--log-file", dest="log_file", help="Path to log file")

    args = parser.parse_args()
    if args.log_file is not None:
        logging.basicConfig(format="", levelname=args.log_level, filename=args.log_file)
    else:
        logging.basicConfig(format="", levelname=args.log_level, stream=sys.stdout)
    try:
        with open(args.config, 'r') as config_file:
            config = yaml.load(config_file.read())
    except IOError, e:
        logging.error(e.message)
        raise

    disk_quota_dict = disk_quota()
    cpu_hours_dict = cpu_hours()
    save_results(disk_quota_dict, cpu_hours_dict, config['statusdb'])


