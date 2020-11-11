import json
import git
import csv
import os
from pathlib import PurePath
from py4j.java_gateway import JavaGateway
import datetime as dt
from dateutil.tz import tzutc

from nshm_toshi_client.rupture_generation_task import RuptureGenerationTask

API_URL = "https://k6lrxgwqj9.execute-api.ap-southeast-2.amazonaws.com/dev/graphql"
S3_URL = "https://nshm-tosh-api-dev.s3.amazonaws.com/"
API_KEY = "TOSHI_API_KEY_DEV"

# uncomment for local S3 testing with `sls s3 start &`
# API_URL = 'http://127.0.0.1:5000/graphql'
# S3_URL = "http://localhost:4569"


def get_repo_heads(rootdir, repos):
    result = {}
    for reponame in repos:
        repo = git.Repo(rootdir.joinpath(reponame))
        headcommit = repo.head.commit
        result[reponame] = headcommit.hexsha
    return result

class CSVResultWriter:
    def __init__(self, file, repos):
        create_names = { #create_args
                     'started': None,
                     'permutationStrategy': None,
                     'openshaCore': None,
                     'openshaCommons': None,
                     'openshaUcerf3': None,
                     'nshmNzOpensha': None,
                     'maxJumpDistance': None,
                     'maxSubSectionLength': None,
                     'maxCumulativeAzimuth': None,
                     'minSubSectionsPerParent': None,
                    }.keys()

        done_names = { #done_args
                     'taskId': None,
                     'duration': None,
                     'result': None,
                     'state': None,
                     'ruptureCount': None,
                     'subsectionCount': None,
                     'clusterConnectionCount': None,
                    }.keys()
        fieldnames = ['outputfile']
        fieldnames.extend(create_names)
        fieldnames.extend(done_names)

        self._file = file
        self._writer = csv.DictWriter(file, fieldnames)
        self._writer.writeheader()

    def writerow(self, **kwargs):
        self._writer.writerow(kwargs)
        self._file.flush() #we want to see the data in the csv ASAP


def ruptureSetMetrics(builder):
    conf = builder.getPlausibilityConfig()
    metrics = {}
    metrics["subsection_count"] = builder.getSubSections().size()
    metrics["rupture_count"] = builder.getRuptures().size()
    metrics["possible_cluster_connections"] = conf.getConnectionStrategy().getClusterConnectionCount()

    # get info form the configuratiion
    conf_diags = json.loads(conf.toJSON())
    conns = 0
    for cluster in conf_diags['connectionStrategy']['clusters']:
        conns += len(cluster.get('connections',[]))
    metrics["cluster_connections"] = conns

    return metrics


def run_task(builder, ruptgen_api, writer, filename, input_data_id, ddw, distance, filekey,
        max_cumulative_azimuth, min_sub_sects_per_parent, strategy):
    t0 = dt.datetime.utcnow()
    outputfile = output_folder.joinpath("ruptset_ddw%s_jump%s_%s_%s_%s_%s.zip" %  (ddw,
        distance, filekey, max_cumulative_azimuth, min_sub_sects_per_parent, strategy))

    #task arguments
    create_args = {
     'started':dt.datetime.now(tzutc()).isoformat(),
     'permutationStrategy': strategy,
     'openshaCore': repoheads['opensha-core'],
     'openshaCommons': repoheads['opensha-commons'],
     'openshaUcerf3': repoheads['opensha-ucerf3'],
     'nshmNzOpensha': repoheads['nshm-nz-opensha'],
     'maxJumpDistance':distance,
     'maxSubSectionLength':ddw,
     'maxCumulativeAzimuth':max_cumulative_azimuth,
     'minSubSectionsPerParent':min_sub_sects_per_parent
    }
    #create new task in toshi_api
    task_id = ruptgen_api.create_task(create_args)

    #link task to the input datafile (*.XML)
    ruptgen_api.link_task_file(task_id, input_data_id, 'READ')

    print("building %s started at %s" % (outputfile, dt.datetime.utcnow().isoformat()), end=' ')

    # Run the task....
    builder\
        .setMaxFaultSections(max_sections)\
        .setMaxJumpDistance(distance)\
        .setPermutationStrategy(strategy)\
        .setMaxSubSectionLength(ddw)\
        .setMinSubSectsPerParent(min_sub_sects_per_parent)\
        .setMaxCumulativeAzimuthChange(max_cumulative_azimuth)\
        .buildRuptureSet(filename)

    #capture task metrics
    duration = (dt.datetime.utcnow() - t0).total_seconds()
    metrics = ruptureSetMetrics(builder)

    #create the output dataset
    builder.writeRuptureSet(str(outputfile))

    #task results
    done_args = {
     'taskId':task_id,
     'duration':duration,
     'result':"SUCCESS",
     'state':"DONE",
     'ruptureCount': metrics["rupture_count"],
     'subsectionCount':metrics["subsection_count"],
     'clusterConnectionCount':metrics["cluster_connections"]
    }

    #csv local backup
    create_args.update(done_args)
    create_args['outputfile'] = outputfile.parts[-1]
    writer.writerow(**create_args)

    #record the completed task
    ruptgen_api.complete_task(done_args)

    #upload the task output
    ruptgen_api.upload_task_file(task_id, outputfile, 'WRITE')
    print("; took %s secs" % (dt.datetime.utcnow() - t0).total_seconds())


def run_tasks(builder, ruptgen_api, writer, output_folder, repoheads, inputs, jump_limits, ddw_ratios, strategies,
            max_cumulative_azimuths, min_sub_sects_per_parents, max_sections = 1000):

    for filekey, filepath in inputs.items():
        filename = str(filepath)
        #store the input data
        input_data_id = ruptgen_api.upload_file(filename)
        for strategy in strategies:
            for distance in jump_limits:
                for max_cumulative_azimuth in max_cumulative_azimuths:
                    for min_sub_sects_per_parent in min_sub_sects_per_parents:
                        for ddw in ddw_ratios:
                            run_task(builder, ruptgen_api, writer, filename, input_data_id, ddw, distance, filekey,
                                    max_cumulative_azimuth, min_sub_sects_per_parent,
                                    strategy)

if __name__ == "__main__":

    #setup the java gateway binding
    gateway = JavaGateway()
    app = gateway.entry_point
    builder = app.getBuilder()

    #get the root path for the task local data
    root_folder = PurePath(os.getcwd())

    repos = ["opensha-ucerf3", "opensha-commons", "opensha-core", "nshm-nz-opensha"]
    #repo_root = root_folder
    output_folder = root_folder.joinpath('tmp').joinpath(dt.datetime.utcnow().isoformat().replace(':','-'))
    os.mkdir(output_folder)

    #setup the csv (backup) task recorder
    writer = CSVResultWriter(open(output_folder.joinpath('results.csv'), 'w'), repos)
    repoheads = get_repo_heads(root_folder, repos)

    headers={"x-api-key":os.getenv(API_KEY)}
    ruptgen_api = RuptureGenerationTask(API_URL, S3_URL, None, with_schema_validation=True, headers=headers)

    ##Test parameters
    inputfiles = {
        "ALL": root_folder.joinpath("nshm-nz-opensha/data/FaultModels/DEMO2_DIPFIX_crustal_opensha.xml"),
        "SANS_TVZ2": root_folder.joinpath("nshm-nz-opensha/data/FaultModels/SANSTVZ2_crustal_opensha.xml")}
    strategies = ['DOWNDIP', 'POINTS'] #, 'UCERF3' == DOWNDIP]
    # strategies = ['POINTS',]
    jump_limits = [0.75, 1.0] #, 2.0, 3.0, 4.0, 4.5, 5.0, 5.1, 5.2, 5.3]
    ddw_ratios = [0.5, 1.0] #, 1.5, 2.0, 2.5]
    min_sub_sects_per_parents = [2,3,4]
    max_cumulative_azimuths = [560.0, 580.0, 600.0]

    #test the tests, nomally 1000 for NZ CFM
    max_sections = 300

    #Run the tasks....
    run_tasks(builder, ruptgen_api, writer, output_folder, repoheads,
        inputfiles, jump_limits, ddw_ratios, strategies,
        max_cumulative_azimuths, min_sub_sects_per_parents,
        max_sections)

    print("Done!")

