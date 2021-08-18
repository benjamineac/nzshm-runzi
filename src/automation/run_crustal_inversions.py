import os
import pwd
import itertools
import stat
from pathlib import PurePath
from subprocess import check_call
from multiprocessing.dummy import Pool

import datetime as dt

from scaling.toshi_api import ToshiApi, CreateGeneralTaskArgs
from scaling.opensha_task_factory import OpenshaTaskFactory
from scaling.file_utils import download_files, get_output_file_id, get_output_file_ids

import scaling.inversion_solution_builder_task

# Set up your local config, from environment variables, with some sone defaults
from scaling.local_config import (OPENSHA_ROOT, WORK_PATH, OPENSHA_JRE, FATJAR,
    JVM_HEAP_MAX, JVM_HEAP_START, USE_API, JAVA_THREADS,
    API_KEY, API_URL, S3_URL, CLUSTER_MODE)


def build_crustal_tasks(general_task_id, rupture_sets, args):
    task_count = 0
    task_factory = OpenshaTaskFactory(OPENSHA_ROOT, WORK_PATH, scaling.inversion_solution_builder_task,
        initial_gateway_port=INITIAL_GATEWAY_PORT,
        jre_path=OPENSHA_JRE, app_jar_path=FATJAR,
        task_config_path=WORK_PATH, jvm_heap_max=JVM_HEAP_MAX, jvm_heap_start=JVM_HEAP_START,
        pbs_ppn=JAVA_THREADS,
        pbs_script=CLUSTER_MODE)

    for (rid, rupture_set_info) in rupture_sets.items():
        for (round, completion_energy, max_inversion_time,
                mfd_equality_weight, mfd_inequality_weight, slip_rate_weighting_type,
                slip_rate_weight, slip_uncertainty_scaling_factor,
                slip_rate_normalized_weight, slip_rate_unnormalized_weight,
                mfd_mag_gt_5_sans, mfd_mag_gt_5_tvz,
                mfd_b_value_sans, mfd_b_value_tvz, mfd_transition_mag,
                seismogenic_min_mag)\
            in itertools.product(
                args['rounds'], args['completion_energies'],  args['max_inversion_times'],
                args['mfd_equality_weights'],  args['mfd_inequality_weights'],  args['slip_rate_weighting_types'],
                args['slip_rate_weights'],  args['slip_uncertainty_scaling_factors'],
                args['slip_rate_normalized_weights'],  args['slip_rate_unnormalized_weights'],
                args['mfd_mag_gt_5_sans'],  args['mfd_mag_gt_5_tvz'],
                args['mfd_b_values_sans'],  args['mfd_b_values_tvz'],  args['mfd_transition_mags'],
                args['seismogenic_min_mags']):

            task_count +=1

            task_arguments = dict(
                round = round,
                config_type = 'crustal',
                rupture_set_file_id=rupture_set_info['id'],
                rupture_set=rupture_set_info['filepath'],
                completion_energy=completion_energy,
                max_inversion_time=max_inversion_time,
                mfd_equality_weight=mfd_equality_weight,
                mfd_inequality_weight=mfd_inequality_weight,
                slip_rate_weighting_type=slip_rate_weighting_type,
                slip_rate_weight=slip_rate_weight,
                slip_uncertainty_scaling_factor=slip_uncertainty_scaling_factor,
                slip_rate_normalized_weight=slip_rate_normalized_weight,
                slip_rate_unnormalized_weight=slip_rate_unnormalized_weight,
                seismogenic_min_mag=seismogenic_min_mag,
                mfd_mag_gt_5_sans=mfd_mag_gt_5,
                mfd_mag_gt_5_tvz=str(float(mfd_mag_gt_5)/10),
                mfd_b_value_sans=mfd_b_value_sans,
                mfd_b_value_tvz=mfd_b_value_tvz,
                mfd_transition_mag=mfd_transition_mag
                )

            job_arguments = dict(
                task_id = task_count,
                round = round,
                java_threads=JAVA_THREADS,
                jvm_heap_max = JVM_HEAP_MAX,
                java_gateway_port=task_factory.get_next_port(),
                working_path=str(WORK_PATH),
                root_folder=OPENSHA_ROOT,
                general_task_id=general_task_id,
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
            return

if __name__ == "__main__":

    t0 = dt.datetime.utcnow()

    # If you wish to override something in the main config, do so here ..
    # WORKER_POOL_SIZE = 3
    WORKER_POOL_SIZE = 1
    JVM_HEAP_MAX = 30
    JAVA_THREADS = 4
    #USE_API = False

    INITIAL_GATEWAY_PORT = 26533 #set this to ensure that concurrent scheduled tasks won't clash

    #If using API give this task a descriptive setting...
    TASK_TITLE = "Inversions: Coulomb D90 with target_min_mag = 7.0"
    TASK_DESCRIPTION = """
    A brief description is needed now that we have all the arguments!
    """

    GENERAL_TASK_ID = None

    headers={"x-api-key":API_KEY}
    #general_api = GeneralTask(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)
    # toshi_api = ToshiFile(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)
    toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)

    #get input files from API
    #upstream_task_id = "R2VuZXJhbFRhc2s6Mjk2MmlTNEs=" #Azimuthal
    #upstream_task_id = "R2VuZXJhbFRhc2s6OTMyNDRibg==" #COulomb NZ CFM 0.3 & 0.9 with current UCERF4 defaults
    #upstream_task_id = "R2VuZXJhbFRhc2s6MjUzQjdjOU4=" #TEST API


    file_id = "RmlsZTozMDMuMEJCOVVY" #PROD D90 Coulomb
    # file_id = "RmlsZTo4NTkuMDM2Z2Rw" #PROD 2010_Coulomb
    # file_id = "RmlsZTo2LjB2NHVOVA==" # DEV LOCAL
    # file_id = "RmlsZToxMzY1LjBzZzRDeA==" #TEST (Subduction)
    file_id = "RmlsZTozODEuMFJxVTI2" #TEST D90
    #file_id = "RmlsZTozMDkuMHB3U0dn" #TEST D90 azimu
    file_id = "RmlsZToxNTg3LjBuVm9GdA==" #TEST D90 full coulomb
    """
    CHOOSE ONE OF:

     - file_generator = get_output_file_id(toshi_api, file_id)
     - file_generator = get_output_file_ids(general_api, upstream_task_id)
    """
    #for a single rupture set, pass a valid FileID
    file_generator = get_output_file_id(toshi_api, file_id) #for file by file ID

    rupture_sets = download_files(toshi_api, file_generator, str(WORK_PATH), overwrite=False)

    args = dict(
        rounds = [str(x) for x in range(1)],
        completion_energies = ['0.0'], # 0.005]
        max_inversion_times = ['10'], #8*60,] #3*60,]  #units are minutes
        #max_inversion_times.reverse()

        #mfd_mag_gt_5s = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200 ]
        mfd_mag_gt_5_sans = ['3.6'],
        mfd_mag_gt_5_tvz = ['0.36'],
        mfd_b_values_sans = ['1.05'],
        mfd_b_values_tvz = ['1.25'],
        mfd_transition_mags = ['7.85'],

        seismogenic_min_mags  = ['7.0'],
        mfd_equality_weights = ['1e3', '1e4'],
        mfd_inequality_weights = ['1e3', '1e4'],

        slip_rate_weighting_types = ['BOTH'], #NORMALIZED_BY_SLIP_RATE', UNCERTAINTY_ADJUSTED',]

        #these are used for UNCERTAINTY_ADJUSTED
        slip_rate_weights = ['', ],# 1e5, 1e4, 1e3, 1e2]
        slip_uncertainty_scaling_factors = ['', ],#2,]

        #these are used for BOTH, NORMALIZED and UNNORMALIZED
        slip_rate_normalized_weights = ['1e3', '1e4'],
        slip_rate_unnormalized_weights = ['1e3', '1e4']
    )
    args_list = []
    for key, value in args.items():
        args_list.append(dict(k=key, v=value))


    if USE_API:
        #create new task in toshi_api
        gt_args = CreateGeneralTaskArgs(
            agent_name=pwd.getpwuid(os.getuid()).pw_name,
            title=TASK_TITLE,
            description=TASK_DESCRIPTION
            )\
            .set_argument_list(args_list)\
            .set_subtask_type('INVERSIONS')\
            .set_model_type('CRUSTAL')


        GENERAL_TASK_ID = toshi_api.general_task.create_task(gt_args)

    print("GENERAL_TASK_ID:", GENERAL_TASK_ID)

    pool = Pool(WORKER_POOL_SIZE)

    scripts = []
    for script_file in build_crustal_tasks(GENERAL_TASK_ID, rupture_sets, args):
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
