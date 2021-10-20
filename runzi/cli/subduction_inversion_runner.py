import json
import os
import pwd
import itertools
import stat
import datetime as dt
from dateutil import tz
from dateutil.tz import tzutc
from unittest import mock
from pathlib import PurePath
from subprocess import check_call
from multiprocessing.dummy import Pool

from runzi.automation.run_subduction_inversions import build_subduction_tasks
from runzi.automation.scaling.toshi_api import ToshiApi, CreateGeneralTaskArgs
from runzi.automation.scaling.opensha_task_factory import OpenshaTaskFactory
from runzi.automation.scaling.file_utils import download_files, get_output_file_id, get_output_file_ids
from runzi.automation.scaling import inversion_solution_builder_task

# Set up your local config, from environment variables, with some sone defaults
from runzi.automation.scaling.local_config import (OPENSHA_ROOT, WORK_PATH, OPENSHA_JRE, FATJAR,
    JVM_HEAP_MAX, JVM_HEAP_START, USE_API, JAVA_THREADS,
    API_KEY, API_URL, S3_URL, CLUSTER_MODE)

def run_subduction_inversion(config):
    t0 = dt.datetime.utcnow()

    WORKER_POOL_SIZE = config._worker_pool_size
    JVM_HEAP_MAX = config._jvm_heap_max
    JAVA_THREADS = config._java_threads
    USE_API = config._use_api
    TASK_TITLE = config._task_title
    TASK_DESCRIPTION = config._task_description
    GENERAL_TASK_ID = config._general_task_id
    MOCK_MODE = config._mock_mode
    file_id = config._file_id
    MODEL_TYPE = config._model_type
    SUBTASK_TYPE = config._subtask_type


    headers={"x-api-key":API_KEY}
    toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)
    
    args = config.get_task_args()
    args_list = []
    for key, value in args.items():
        args_list.append(dict(k=key, v=value))

    file_generator = get_output_file_id(toshi_api, file_id) #for file by file ID
    rupture_sets = download_files(toshi_api, file_generator, str(WORK_PATH), overwrite=False)
    
    if USE_API:
    #create new task in toshi_api
        gt_args = CreateGeneralTaskArgs(
            agent_name=pwd.getpwuid(os.getuid()).pw_name,
            title=TASK_TITLE,
            description=TASK_DESCRIPTION
            )\
            .set_argument_list(args_list)\
            .set_subtask_type(SUBTASK_TYPE)\
            .set_model_type(MODEL_TYPE)

        GENERAL_TASK_ID = toshi_api.general_task.create_task(gt_args)    

    print("GENERAL_TASK_ID:", GENERAL_TASK_ID)

    scripts = []
    for script_file in build_subduction_tasks(GENERAL_TASK_ID, rupture_sets, args):
        # print('scheduling: ', script_file)
        scripts.append(script_file)

    def call_script(script_name):
        print("call_script with:", script_name)
        if CLUSTER_MODE:
            check_call(['qsub', script_name])
        else:
            check_call(['bash', script_name])


    print('task count: ', len(scripts))
    print('worker count: ', WORKER_POOL_SIZE)

    if MOCK_MODE:
        call_script = mock.Mock(call_script)

    pool = Pool(WORKER_POOL_SIZE)
    pool.map(call_script, scripts)
    pool.close()
    pool.join()

    print("Done! in %s secs" % (dt.datetime.utcnow() - t0).total_seconds())
