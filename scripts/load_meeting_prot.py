"""This script loada info from trello and merge with info from latest meeting 
protocol. It then loads a new meeting protocol with the merged info devided into
comming and ongoing deliveries. The info consists of project name,project info, 
flow cell ids and comments."""

import bcbio.google
import bcbio.google.spreadsheet
import sys
import os
import operator
from optparse import OptionParser
from datetime import date, timedelta

CREDENTIALS_FILE = os.path.join(os.environ['HOME'], 'opt/config/gdocs_credentials')
credentials = bcbio.google.get_credentials({'gdocs_upload': {'gdocs_credentials': CREDENTIALS_FILE}})
CLIENT = bcbio.google.spreadsheet.get_client(credentials)

def get_ws(wsheet_title,ssheet):
    wsheet = bcbio.google.spreadsheet.get_worksheet(CLIENT,ssheet,wsheet_title)
    assert wsheet is not None, "Could not find worksheet %s within spreadsheet %s" % (wsheet_title,ssheet)
    content = bcbio.google.spreadsheet.get_cell_content(CLIENT,ssheet,wsheet)
    ws_key = bcbio.google.spreadsheet.get_key(wsheet)
    return ws_key, content

def get_meeting_info_from_wsheet(content):
    "Feching info from old metting protocol. Example to show data structure:

    If google meeting protocol looks like this:

    |--------------------|--------------------------------------------|---------------------|
    |col A               |col B                                       |col c                |
    |--------------------|--------------------------------------------|---------------------|
    |C.Dixelius_13_04    |- Production - Jun - RNA-seq (total RNA)    |                     |
    |                    |140228_SN1025_0204_BC3TYRACXX               |FAILed run           |
    |                    |140314_SN1025_0206_BC3YL6ACXX               |Delivery onhold.     |
    |                    |140407_SN7001301_0127_BH8M8MADXX            |Delivered and closed |
    |                    |                                            |                     |
    |CM.Dixelius_13_05   |- Production - Par - de novo                |                     |
    |                    |140314_SN1025_0206_BC3YL6ACXX               |deliv to Application |
    |                    |140411_SN7001362_0119_BC42B3ACXX            |Delivered and closed |
    |--------------------|--------------------------------------------|---------------------|

    data will look like this:
    
    data = {C.Dixelius_13_04 : 
                {'info' : '- Production - Jun - RNA-seq (total RNA)', 
                 'flowcells' : {'140228_SN1025_0204_BC3TYRACXX' : 'FAILed run',
                                '140314_SN1025_0206_BC3YL6ACXX' : 'Delivery onhold.',
                                '140407_SN7001301_0127_BH8M8MADXX' : 'Delivered and closed'}},
            CM.Dixelius_13_05 : 
                {'info' : '- Production - Par - de novo ', 
                 'flowcells' : {'140228_SN1025_0204_BC3TYRACXX' : 'deliv to Application',
                                '140411_SN7001362_0119_BC42B3ACXX' : 'Delivered and closed '}}}"
    flow_cell = None
    data = {}
    for row in content:
        col_A = row[0].strip()
        col_B = row[1].strip()
        col_C = row[2].strip()
        if col_A and col_A != 'Ongoing deliveries':
            proj_name = col_A
            data[proj_name] = {'info': col_B, 'flowcells':{}}
        elif col_B:
            flow_cell = col_B
            data[proj_name]['flowcells'][flow_cell] = [col_C]
        elif col_C and flow_cell:
            data[proj_name]['flowcells'][flow_cell].append(col_C)
    return data

def sort_by_name(namelist):
    "Sorts dict alphabeticly by project sure name"
    name_dict = {}
    for proj_name in namelist:
        name_dict[proj_name] = proj_name.split('.')[1].strip()
    sorted_name_dict = sorted(name_dict.iteritems(), key=operator.itemgetter(1))
    return sorted_name_dict

def merge_info_from_file_and_wsheet(trello_dump, old_wsheet_content):
    """Collects info from old meeting protocol with get_meeting_info_from_wsheet.
    Then parses trello_dump - output from script "update_checklist.py", which is
    loading runinfo from the trello board. 
    Merges the info from the old meeting prot with the info from the trello dump.
    Returns a dict with information about comming and ongoing deliveries.
        ongoing:    ongoing_projects is a dict with info from old meeting that 
                    is merged with new info feched from trello. A project is
                    ongoing if it has a flowcell that has been sequenced and 
                    demultiplexed. 
                    Dict structure as described in get_meeting_info_from_wsheet.
        comming:    coming_projects is a dict with info feched from trello.
                    A project is ongoing if it has no flowcells that has been 
                    sequenced and demultiplexed, but has flocells that are 
                    currently being so. 
                    Dict structure as described in get_meeting_info_from_wsheet."""

    ongoing_projects = get_meeting_info_from_wsheet(old_wsheet_content)
    f = open(trello_dump,'r')
    content = f.readlines()
    coming_projects = {}
    dict_holder = coming_projects
    proj_name = None
    for row in content:
        row = row.strip()
        if row == "Ongoing":
            dict_holder = ongoing_projects
        if row:
            if len(row) > 2 and row[1] == '.' or row[2] == '.':
                row_list = row.split()
                proj_name = row_list[0]
                info = ' '.join(row_list[1:])
                if not proj_name in dict_holder.keys():
                    dict_holder[proj_name] = {'info' : info, 'flowcells' : {}}
            elif proj_name and not row[0].isdigit():
                if dict_holder[proj_name].has_key('info'):
                    if row not in dict_holder[proj_name]['info']:
                        info = dict_holder[proj_name]['info']
                        more_info = ' - '.join([info, row])
                        dict_holder[proj_name]['info'] = more_info
            elif proj_name and row[0].isdigit():
                if not dict_holder[proj_name]['flowcells'].has_key(row):
                    dict_holder[proj_name]['flowcells'][row] = []
                proj_name = None
    return {'coming' : coming_projects, 'ongoing' : ongoing_projects}

def update(project_status, new_meeting_content, ss_key, ws_key):
    """Uppdates the new meeting protocol with old and new info stored in 
    new_meeting_content - a dictionary structured as described in 
    get_meeting_info_from_wsheet"""

    if project_status == 'ongoing':
        col = 1 # starts updating ws from first kolumn if project is ongoing
        meeting_info_dict = new_meeting_content['ongoing']
    elif project_status == 'coming':
        col = 4 # starts updating ws from fourth kolumn if project is comming
        meeting_info_dict = new_meeting_content['coming']

    sorted_names = sort_by_name(meeting_info_dict.keys())
    row = 2     # starts at the second row in the ws (first row contains headers)
    for proj_name in sorted_names:
        proj_name = proj_name[0]
        # updates project name in column col and project type info in column col+1 
        CLIENT.UpdateCell(row, col, proj_name , ss_key, ws_key)
        CLIENT.UpdateCell(row, col+1, meeting_info_dict[proj_name]['info'] , 
                                                            ss_key, ws_key)
        sorted_fcs = sorted(meeting_info_dict[proj_name]['flowcells'].keys())
        for fc in sorted_fcs:
            row += 1
            comments_row = row
            # updates flowcell name in column col+1 
            CLIENT.UpdateCell(row, col+1, fc, ss_key, ws_key)
            for comment in meeting_info_dict[proj_name]['flowcells'][fc]:
                # updates comments per flowcell in column col+2 
                CLIENT.UpdateCell(comments_row, col+2, comment, ss_key, ws_key)
                comments_row += 1
            if row != comments_row:
                row = comments_row - 1
        # adds an empty row after every project
        row += 2

def main(old_wsheet, new_wsheet, file_dump, ssheet_title):
    ssheet = bcbio.google.spreadsheet.get_spreadsheet(CLIENT, ssheet_title)
    assert ssheet is not None, "Could not find spreadsheet %s" % ssheet_title
    ss_key = bcbio.google.spreadsheet.get_key(ssheet)
    dummy, old_wsheet_content = get_ws(old_wsheet, ssheet)
    new_ws_key, dummy = get_ws(new_wsheet, ssheet)

    new_wsheet_content = merge_info_from_file_and_wsheet(file_dump, old_wsheet_content)
    update(project_status = 'ongoing', new_wsheet_content, ss_key, new_ws_key)
    update(project_status = 'coming' , new_wsheet_content, ss_key, new_ws_key)

if __name__ == "__main__":
    parser = OptionParser(usage = """load_meeting_prot.py <Arguments> [Options]

Arguments:    
  trello_dump           File storing info about new runs. Give the whole file 
                        path! The file is generated by running: 
                        update_checklist.py hugin-conf.yaml >> trello_dump""")
    parser.add_option("-o", "--old_wsheet", dest="old_wsheet", 
        default = (date.today() - timedelta(days = 7)).isoformat(), 
        help = "Get old notes from this wsheet.")
    parser.add_option("-n", "--new_wsheet", dest="new_wsheet", 
        default=date.today().isoformat(), 
        help = "Load this wsheet with old notes from old wsheet and new notes from the trello dump.")
    parser.add_option("-s", "--ssheet_title", dest="ssheet_title", 
        default = str('Bioinformatics_Meeting_Deliveries_' + str(date.today().year)),
        help = "Name of target spredsheet. Default is Bioinformatics_Meeting_Deliveries_[current year]")
    (options, args) = parser.parse_args()
    if not args: sys.exit('Missing requiered argument <trello_dump>')
    else: file_dump =  args[0]
    main(options.old_wsheet, options.new_wsheet, file_dump, options.ssheet_title)

