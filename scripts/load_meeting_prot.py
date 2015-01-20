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

def get_meeting_info(content):
    "Feching info from old metting protocol"
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

def pars_file(file, ongoing_deliveries, content):
    """parses output from script "update_checklist.py", which is loading runinfo
    from the trello board. ongoing_deliverues is a dict of info from old meeting
    that will be merged with the new info feched from trello."""
    f = open(file,'r')
    content = f.readlines()
    coming_deliveries = {}
    dict_holder = coming_deliveries
    proj_name = None
    for row in content:
        row = row.strip()
        if row == "Ongoing":
            dict_holder = ongoing_deliveries
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
    return coming_deliveries, ongoing_deliveries

def update(sorted_names, col, info, ss_key, ws_key):
    """Uppdates the new meeting protocol with old and new info."""
    row = 2
    for proj_name in sorted_names:
        proj_name = proj_name[0]
        CLIENT.UpdateCell(row, col, proj_name , ss_key, ws_key)
        CLIENT.UpdateCell(row, col+1, info[proj_name]['info'] , ss_key, ws_key)
        sorted_fcs = sorted(info[proj_name]['flowcells'].keys())
        for fc in sorted_fcs:
            row += 1
            comments_row = row
            CLIENT.UpdateCell(row, col+1, fc, ss_key, ws_key)
            for comment in info[proj_name]['flowcells'][fc]:
                CLIENT.UpdateCell(comments_row, col+2, comment, ss_key, ws_key)
                comments_row += 1
            if row != comments_row:
                row = comments_row - 1
        row += 2

def main(old_wsheet, new_wsheet, file_dump, ssheet_title):
    ssheet = bcbio.google.spreadsheet.get_spreadsheet(CLIENT,ssheet_title)
    assert ssheet is not None, "Could not find spreadsheet %s" % ssheet_title
    ss_key = bcbio.google.spreadsheet.get_key(ssheet)
    ws_key, content = get_ws(old_wsheet,ssheet)
    ongoing_deliveries = get_meeting_info(content)
    coming_deliveries, ongoing_deliveries = pars_file(file_dump, ongoing_deliveries, content)
    sorted_ongoing_names = sort_by_name(ongoing_deliveries.keys())
    sorted_comming_names = sort_by_name(coming_deliveries.keys())
    ws_key, content = get_ws(new_wsheet,ssheet)
    update(sorted_ongoing_names, 1, ongoing_deliveries, ss_key, ws_key)
    update(sorted_comming_names, 4, coming_deliveries,ss_key, ws_key)

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

