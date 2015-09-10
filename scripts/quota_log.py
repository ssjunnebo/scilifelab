"""Generates dictionaries for the load on the quota
using 'uquota'. If a couchdb is specified, the dictionaries will be sent there.
Otherwise prints the dictionaries.
"""
import datetime
import subprocess
import couchdb
import yaml

def disk_quota():
    current_time = datetime.datetime.now()
    uq = subprocess.Popen(["/bubo/sw/uppmax/bin/uquota", "-q"], stdout=subprocess.PIPE)

    output = uq.communicate()[0]

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

    # script that runs on uppmax
    uq = subprocess.Popen(["/sw/uppmax/bin/projinfo", '-q'], stdout=subprocess.PIPE)

    # output is lines with the format: project_id  cpu_usage  cpu_limit
    output = uq.communicate()[0]
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


    # create db instance
    server = "http://{username}:{password}@{url}:{port}".format(
        url=db_config['url'],
        username=db_config['username'],
        password=db_config['password'],
        port=db_config['port'])

    couch = couchdb.Server(server)
    db = couch['uppmax']

    # save results in the database
    for project in merged_results.values():
        db.save(project)



if __name__ == "__main__":
    with open('/home/funk_001/.ngi_config/statusdb.yaml', 'r') as config_file:
        config = yaml.load(config_file.readall())

    disk_quota_dict = disk_quota()
    cpu_hours_dict = cpu_hours()
    save_results(disk_quota_dict, cpu_hours_dict, config['statusdb'])


