import argparse
import json
import git
import os
from pathlib import PurePath
import platform

from py4j.java_gateway import JavaGateway, GatewayParameters
import datetime as dt
from dateutil.tz import tzutc
from types import SimpleNamespace

from nshm_toshi_client.rupture_generation_task import RuptureGenerationTask
from nshm_toshi_client.general_task import GeneralTask
from nshm_toshi_client.task_relation import TaskRelation
import time

API_URL  = os.getenv('NZSHM22_TOSHI_API_URL', "http://127.0.0.1:5000/graphql")
API_KEY = os.getenv('NZSHM22_TOSHI_API_KEY', "")
S3_URL = os.getenv('NZSHM22_TOSHI_S3_URL',"http://localhost:4569")

class BuilderTask():
    """
    The python client for a RuptureSetBuildTask
    """
    def __init__(self, job_args):

        self.use_api = job_args.get('use_api', False)

        #setup the java gateway binding
        self._gateway = JavaGateway(gateway_parameters=GatewayParameters(port=job_args['java_gateway_port']))
        self._inversion_runner = self._gateway.entry_point.getRunner()

        repos = ["opensha", "nshm-nz-opensha"]
        self._repoheads = get_repo_heads(PurePath(job_args['root_folder']), repos)
        self._output_folder = PurePath(job_args.get('working_path'))

        if self.use_api:
            headers={"x-api-key":API_KEY}
            self._ruptgen_api = RuptureGenerationTask(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)
            self._general_api = GeneralTask(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)
            self._task_relation_api = TaskRelation(API_URL, None, with_schema_validation=True, headers=headers)

    def run(self, task_arguments, job_arguments):

        # print(task_arguments)
        # print(job_arguments)

        t0 = dt.datetime.utcnow()

        environment = {
            "host": platform.node(),
            "gitref_opensha":self._repoheads['opensha'],
            "gitref_nshm-nz-opensha":self._repoheads['nshm-nz-opensha'] }

        if self.use_api:
            #create new task in toshi_api
            task_id = self._ruptgen_api.create_task(
                dict(created=dt.datetime.now(tzutc()).isoformat()),
                arguments=task_arguments,
                environment=environment
                )

            #link task tp the parent task
            self._task_relation_api.create_task_relation(job_arguments['general_task_id'], task_id)

            # #link task to the input datafile
            input_file_id = task_arguments.get('rupture_set_file_id')
            if input_file_id:
                self._ruptgen_api.link_task_file(task_id, input_file_id, 'READ')

        else:
            task_id = None

        # Run the task....
        ta = task_arguments

        print("Starting inversion of up to %s minutes" % ta['max_inversion_time'])
        print("======================================")
        self._inversion_runner\
            .setInversionSeconds(int(ta['max_inversion_time'] * 60))\
            .setEnergyChangeCompletionCriteria(float(0), float(ta['completion_energy']), float(1))\
            .setNumThreads(int(job_arguments["java_threads"]))\
            .setSyncInterval(30)\
            .setRuptureSetFile(str(PurePath(job_arguments['working_path'], ta['rupture_set'])))

        mfd = SimpleNamespace(**dict(
            total_rate_m5 = 8.8,
            b_value = 1.0,
            mfd_transition_mag = 7.85,
            mfd_num = 40,
            mfd_min = 5.05,
            mfd_max = 8.95))

        mfd_equality_constraint_weight = 10
        mfd_inequality_constraint_weight = 1000

        sliprate_weighting = self._gateway.jvm.UCERF3InversionConfiguration.SlipRateConstraintWeightingType

        # .setGutenbergRichterMFD(mfd.total_rate_m5, mfd.b_value, mfd.mfd_transition_mag, mfd.mfd_num, mfd.mfd_min, mfd.mfd_max)
        # .setSlipRateConstraint(sliprate_weighting.NORMALIZED_BY_SLIP_RATE, float(100), float(10))\
        # .setSlipRateUncertaintyConstraint(sliprate_weighting.UNCERTAINTY_ADJUSTED, 1000, 2)\

        self._inversion_runner\
            .setGutenbergRichterMFDWeights(
                 float(mfd_equality_constraint_weight),
                 float(mfd_inequality_constraint_weight))\
            .configure()\
            .runInversion()

        #output_file = str(PurePath(job_arguments['working_path'], f"SOLUTION_FILE_{job_arguments['java_gateway_port']}.zip"))
        output_file = str(PurePath(job_arguments['output_file']))
        self._inversion_runner.writeSolution(output_file)

        t1 = dt.datetime.utcnow()
        print("Inversion took %s secs" % (t1-t0).total_seconds())

        #capture task metrics
        duration = (dt.datetime.utcnow() - t0).total_seconds()

        metrics = {}
        # metrics['completion_criteria'] = self._inversion_runner.completionCriteriaMetrics()
        # metrics['moment_rate'] = self._inversion_runner.momentAndRateMetrics()
        # metrics['by_fault_name'] = self._inversion_runner.byFaultNameMetrics()
        # metrics['parent_fault_moment_rates'] = self._inversion_runner.parentFaultMomentRates()

        if self.use_api:
            #record the completed task
            done_args = {
             'task_id':task_id,
             'duration':duration,
             'result':"SUCCESS",
             'state':"DONE",
            }
            self._ruptgen_api.complete_task(done_args, metrics)

            #upload the task output
            self._ruptgen_api.upload_task_file(task_id, output_file, 'WRITE', meta=task_arguments)

            #and the log files, why not
            java_log_file = self._output_folder.joinpath(f"java_app.{job_arguments['java_gateway_port']}.log")
            self._ruptgen_api.upload_task_file(task_id, java_log_file, 'WRITE')
            pyth_log_file = self._output_folder.joinpath(f"python_script.{job_arguments['java_gateway_port']}.log")
            self._ruptgen_api.upload_task_file(task_id, pyth_log_file, 'WRITE')

        else:
            print(metrics)
        print("; took %s secs" % (dt.datetime.utcnow() - t0).total_seconds())


def get_repo_heads(rootdir, repos):
    result = {}
    for reponame in repos:
        repo = git.Repo(rootdir.joinpath(reponame))
        headcommit = repo.head.commit
        result[reponame] = headcommit.hexsha
    return result

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    args = parser.parse_args()

    config_file = args.config
    f= open(config_file, 'r', encoding='utf-8')
    config = json.load(f)

    # maybe the JVM App is a little slow to get listening
    time.sleep(5)
    # Wait for some more time, scaled by taskid to avoid S3 consistency issue
    time.sleep(config['job_arguments']['task_id'] * 0.333 * 2 * 4)

    # print(config)
    task = BuilderTask(config['job_arguments'])
    task.run(**config)
