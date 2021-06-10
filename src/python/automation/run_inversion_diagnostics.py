import os
import pwd
import itertools
import stat
from pathlib import PurePath
from subprocess import check_call
from multiprocessing.dummy import Pool

import datetime as dt
from dateutil.tz import tzutc

from nshm_toshi_client.general_task import GeneralTask
from nshm_toshi_client.toshi_file import ToshiFile

from scaling.opensha_task_factory import OpenshaTaskFactory
from scaling.file_utils import download_files

import scaling.diagnostics_report_task


# Set up your local config, from environment variables, with some sone defaults
from scaling.local_config import (OPENSHA_ROOT, WORK_PATH, OPENSHA_JRE, FATJAR,
    JVM_HEAP_MAX, JVM_HEAP_START, USE_API, JAVA_THREADS,
    API_KEY, API_URL, S3_URL, CLUSTER_MODE)

# If you wish to override something in the main config, do so here ..
# WORKER_POOL_SIZE = 3
WORKER_POOL_SIZE = 2
JVM_HEAP_MAX = 12
JAVA_THREADS = 4
USE_API = True


#If using API give this task a descriptive setting...
TASK_TITLE = "Baseline Inversion - Coulomb"
TASK_DESCRIPTION = """
- Coulomb rupture sets
- Fixed duration comparisons
"""

def run_tasks(general_task_id, rupture_sets, rupture_class):
    task_count = 0
    task_factory = OpenshaTaskFactory(OPENSHA_ROOT, WORK_PATH, scaling.diagnostics_report_task,
        jre_path=OPENSHA_JRE, app_jar_path=FATJAR,
        task_config_path=WORK_PATH, jvm_heap_max=JVM_HEAP_MAX, jvm_heap_start=JVM_HEAP_START,
        pbs_script=CLUSTER_MODE)

    for (rid, rupture_set_info) in rupture_sets.items():

        task_count +=1

        short_name = rupture_set_info['info'].get('short_name') or \
            f"{rupture_set_info['info']['fault_model']}-{rupture_set_info['info']['thinning_factor']}" # for old pre-short_name

        #rupture_set_info['info'] has detaail of the Inversion task
        task_arguments = dict(
            rupture_set_file_id = str(rupture_set_info['id']),
            generation_task_id = str(rupture_set_info['info']['generation_task_id']),
            # report_name = str(PurePath(rupture_set_info['filepath']).name).replace('.zip', '') + "-" + rupture_set_info['info']['max_inversion_time'],
            solution_file = str(rupture_set_info['filepath']),
            short_name = short_name, #includes RuptureSetENUM + thinning factor
            rupture_class = rupture_class,
            max_inversion_time = rupture_set_info['info']['max_inversion_time'],
            completion_energy = rupture_set_info['info']['completion_energy'],
            round_number = rupture_set_info['info']['round'],
            )

        job_arguments = dict(
            task_id = task_count,
            # round = round,
            java_threads = JAVA_THREADS,
            java_gateway_port = task_factory.get_next_port(),
            working_path = str(WORK_PATH),
            root_folder = OPENSHA_ROOT,
            general_task_id = general_task_id,
            use_api = USE_API,
            )

        #write a config
        task_factory.write_task_config(task_arguments, job_arguments)

        script = task_factory.get_task_script()

        script_file_path = PurePath(WORK_PATH, f"task_{task_count}.sh")
        with open(script_file_path, 'w') as f:
            f.write(script)

        #make file executable
        st = os.stat(script_file_path)
        os.chmod(script_file_path, st.st_mode | stat.S_IEXEC)

        yield str(script_file_path)


if __name__ == "__main__":

    t0 = dt.datetime.utcnow()

    GENERAL_TASK_ID = None

    if USE_API:
        headers={"x-api-key":API_KEY}
        general_api = GeneralTask(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)
        file_api = ToshiFile(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)

        #get input files from API
        #upstream_task_id = "R2VuZXJhbFRhc2s6NjI3M2E5QWc=" #PROD Coulomb Inversions May 26
        #upstream_task_id = "R2VuZXJhbFRhc2s6NjMyUzRDZGM=" #TEST  Coulomb Inversions May 25
        #upstream_task_id = "R2VuZXJhbFRhc2s6OThUR2YybQ==" #Azimuthal (named Couldmb!!)
        #upstream_task_id = "R2VuZXJhbFRhc2s6MTIwVUxQOFY=" #PROD Coulomb May 27

        #upstream_task_id = "R2VuZXJhbFRhc2s6MTM3Uk5UUjU=" #PROD Coulomb May 29
        upstream_task_id = "R2VuZXJhbFRhc2s6MTYyb2k0TGQ==" #PROD Azimuthal May 29

        rupture_sets = download_files(general_api, file_api, upstream_task_id, str(WORK_PATH), id_suffix=True, overwrite=False)

        # #create new task in toshi_api
        # GENERAL_TASK_ID = general_api.create_task(
        #     created=dt.datetime.now(tzutc()).isoformat(),
        #     agent_name=pwd.getpwuid(os.getuid()).pw_name,
        #     title=TASK_TITLE,
        #     description=TASK_DESCRIPTION
        # )

        print("GENERAL_TASK_ID:", GENERAL_TASK_ID)

    #print( rupture_sets )
    #assert 0

    pool = Pool(WORKER_POOL_SIZE)

    RUPTURE_CLASS = "Azimuth" #### TODO FIX THIS it comes fomr the data!!

    scripts = []
    for script_file in run_tasks(GENERAL_TASK_ID, rupture_sets, RUPTURE_CLASS):
        print('scheduling: ', script_file)
        scripts.append(script_file)

    def call_script(script_name):
        print("call_script with:", script_name)
        if CLUSTER_MODE:
            check_call(['qsub', script_name])
        else:
            check_call(['bash', script_name])

    print('task count: ', len(scripts))
    print('worker count: ', WORKER_POOL_SIZE)

    pool.map(call_script, scripts)
    pool.close()
    pool.join()

    print("Done! in %s secs" % (dt.datetime.utcnow() - t0).total_seconds())
