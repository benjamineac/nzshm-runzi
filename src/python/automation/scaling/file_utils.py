#!python3
"""
helpers for upstream file retrieval

"""
import os
import requests
from pathlib import PurePath


def get_output_file_ids(general_task_api, upstream_task_id, file_extension='zip'):

    api_result = general_task_api.get_subtask_files(upstream_task_id)
    for subtask in api_result['children']['edges']:
        for filenode in subtask['node']['child']['files']['edges']:
            if filenode['node']['file']['file_name'][-3:] == file_extension:

                short_name = None

                for kv in filenode['node']['file'].get('meta', []):
                    if kv.get('k') == 'short_name':
                        short_name = kv.get('v')
                        break

                yield dict(id = filenode['node']['file']['id'],
                        file_name = filenode['node']['file']['file_name'],
                        file_size = filenode['node']['file']['file_size'],
                        short_name = short_name)


def get_download_info(file_api, file_infos):
    """
    [{'id': 'RmlsZToyOS4wRUVjV0E=',
    'file_name': 'RupSet_Cl_FM(CFM_0_3_SANSTVZ)_noInP(T)_slRtP(0.05)_slInL(F)_cfFr(0.75)_cfRN(2)_cfRTh(0.5)_cfRP(0.01)_fvJm(T)_jmPTh(0.001)_cmRkTh(360)_mxJmD(15)_plCn(T)_adMnD(6)_adScFr(0)_bi(F)_stGrSp(2)_coFr(0.5).zip',
    'file_size': 2498443,
    'short_name': None}]
    """
    file_info = {}
    for itm in file_infos:
        api_result = file_api.get_download_url(itm['id'])
        # print(api_result)
        yield dict(dict(file_url=api_result['file_url']), **itm) #merge the discts

def download_files(general_api, file_api, upstream_task_id, dest_folder, overwrite=False):

    downloads = dict()

    for info in get_download_info(file_api, get_output_file_ids(general_api, upstream_task_id)):

        #we can skip if file exists and has correct file_size
        file_path = PurePath(dest_folder, info['file_name'])
        shortname = info['short_name'] or info['id']
        downloads[shortname] = dict(id=info['id'], filepath = str(file_path))

        if os.path.isfile(file_path):
            if os.path.getsize(file_path) == info['file_size']:
                #file exists and has correct size
                if not overwrite:
                    continue

        # here we pull the file
        # validate the file size
        print(info['file_url'])
        # r0 = requests.head(info['file_url'])

        r1 = requests.get(info['file_url'])
        with open(str(file_path), 'wb') as f:
            f.write(r1.content)
            print("downloaded input file:", file_path, f)
            os.path.getsize(file_path) == info['file_size']

    return downloads