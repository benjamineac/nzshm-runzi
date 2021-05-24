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
from scaling.opensha_task_factory import OpenshaTaskFactory

# Set up your local config, from environment variables, with some sone defaults
from local_config import (OPENSHA_ROOT, WORK_PATH, OPENSHA_JRE, FATJAR,
    JVM_HEAP_MAX, JVM_HEAP_START, USE_API, JAVA_THREADS,
    API_KEY, API_URL, S3_URL, CLUSTER_MODE)

# If you wish to override something in the main config, do so here ..
# WORKER_POOL_SIZE = 3
WORKER_POOL_SIZE = 1
JVM_HEAP_MAX = 32
JAVA_THREADS = 4
USE_API = True

#If using API give this task a descriptive setting...
TASK_TITLE = "Baseline Inversion energy completion"
TASK_DESCRIPTION = """
Test inversion energy Completion impacts:

Fixed duration comparisons
"""

def run_tasks(general_task_id, rupture_sets, completion_energies, max_inversion_times):
    task_count = 0
    task_factory = OpenshaTaskFactory(OPENSHA_ROOT, WORK_PATH, python_script="inversion_solution_builder_task.py",
        jre_path=OPENSHA_JRE, app_jar_path=FATJAR,
        task_config_path=WORK_PATH, jvm_heap_max=JVM_HEAP_MAX, jvm_heap_start=JVM_HEAP_START,
        pbs_script=CLUSTER_MODE)

    for round in rounds:
        for (rid, rupture_set) in rupture_sets.items():
            for completion_energy in completion_energies:
                for max_inversion_time in max_inversion_times:
    
                    task_count +=1
    
                    task_arguments = dict(                     
                        round = round,
                        rupture_set=rupture_set,
                        completion_energy=completion_energy,
                        max_inversion_time=max_inversion_time,
                        )

                    job_arguments = dict(
                        task_id = task_count,
                        round = round,
                        java_threads=JAVA_THREADS,
                        java_gateway_port=task_factory.get_next_port(),
                        working_path=str(WORK_PATH),
                        root_folder=OPENSHA_ROOT,
                        general_task_id=general_task_id,
                        use_api = USE_API,
                        output_file = f"{str(WORK_PATH)}/InversionSolution-{str(rid)}-rnd{round}-t{max_inversion_time}.zip",
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
        #create new task in toshi_api
        GENERAL_TASK_ID = general_api.create_task(
            created=dt.datetime.now(tzutc()).isoformat(),
            agent_name=pwd.getpwuid(os.getuid()).pw_name,
            title=TASK_TITLE,
            description=TASK_DESCRIPTION
        )

    ##Parameters
    rupt_folder = "/home/chrisch/NSHM/opensha-new/work/save/"
    rupture_sets = {
        "CFM09_tf0.0": rupt_folder + "RupSet_Az_FM(CFM_0_9_SANSTVZ_D90)_mxSbScLn(0.5)_mxAzCh(60.0)_mxCmAzCh(560.0)_mxJpDs(5.0)_mxTtAzCh(60.0)_thFc(0.0).zip",
        "CFM09_tf0.1": rupt_folder +"RupSet_Az_FM(CFM_0_9_SANSTVZ_D90)_mxSbScLn(0.5)_mxAzCh(60.0)_mxCmAzCh(560.0)_mxJpDs(5.0)_mxTtAzCh(60.0)_thFc(0.1).zip",
        "CFM03_tf0.0": rupt_folder +"RupSet_Az_FM(CFM_0_3_SANSTVZ)_mxSbScLn(0.5)_mxAzCh(60.0)_mxCmAzCh(560.0)_mxJpDs(5.0)_mxTtAzCh(60.0)_thFc(0.0).zip",
        "CFM03_tf0.1": rupt_folder +"RupSet_Az_FM(CFM_0_3_SANSTVZ)_mxSbScLn(0.5)_mxAzCh(60.0)_mxCmAzCh(560.0)_mxJpDs(5.0)_mxTtAzCh(60.0)_thFc(0.1).zip",
    }

    rounds = range(3)
    completion_energies = [0.000001,] #, 0.2] # 0.1, 0.001]
    max_inversion_times = [10, 30, 60, 120, 4*60, 8*60, 16*60,]  #units are minutes
    max_inversion_times.reverse()

    pool = Pool(WORKER_POOL_SIZE)

    scripts = []
    for script_file in run_tasks(GENERAL_TASK_ID, rupture_sets, completion_energies, max_inversion_times):
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
