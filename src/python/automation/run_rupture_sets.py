import json
import git
import csv
import os
from pathlib import PurePath
from py4j.java_gateway import JavaGateway
from datetime import datetime as dt
# from cgitb import reset

def get_repo_heads(rootdir, repos):
	result = {}
	for reponame in repos:
		repo = git.Repo(rootdir.joinpath(reponame))
		headcommit = repo.head.commit		
		result[reponame] = headcommit.hexsha
	return result

# def get_git_head_meta(rootdir, repo):
# 	repo = git.Repo(rootdir.joinpath(repo))
# 	headcommit = repo.head.commit
# 	return dict(
# 		head_commit_date = dt.fromtimestamp(headcommit.committed_date).isoformat(),
# 		head_hexsha = headcommit.hexsha,
# 		head_author_name = headcommit.author.name )

class CSVResultWriter:
	def __init__(self, file, repos):
		fieldnames = ['Data', 'OutputFile', 'PermutationStrategy', 'maxJumpDistance', 'maxSubSectionLength', 'maxFaultSections',
					'minSubSectsPerParent', 'maxCumAz', 'rupture_count', 'subsection_count', 
					'possible_cluster_connections', 'cluster_connections', 'datetime', 'duration'] + repos
		self._file = file
		self._writer = csv.DictWriter(file, fieldnames)
		self._writer.writeheader()
		
	def writerow(self, **kwargs):
		self._writer.writerow(kwargs)
		self._file.flush() #we want to see the data in the csv ASAP 

def run_tests(builder, writer, output_folder, repoheads, inputs, jump_limits, ddw_ratios, strategies, max_sections = 1000):
	
	def ruptureSetMetrics(builder):		
		conf = builder.getPlausabilityConfig()
		
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
	
	def test_arguments(builder, filekey, outputfile, strategy, max_distance, ddw, max_sections, minSubSectsPerParent=2, MaxCumAz=560):
		res = {}
		res['Data'] = filekey
		res['OutputFile'] = outputfile.parts[-1]
		res['PermutationStrategy'] = strategy
		res['maxSubSectionLength'] = ddw
		res['maxFaultSections'] = max_sections
		res['minSubSectsPerParent'] = minSubSectsPerParent
		res['maxCumAz'] = MaxCumAz
		res['maxJumpDistance'] = max_distance
		return res
	
	for key, filepath in inputs.items():
		filename = str(filepath)
		for strategy in strategies:
			for distance in jump_limits:
				for ddw in ddw_ratios:
					t0 = dt.utcnow()
					outputfile = output_folder.joinpath("ruptset_ddw%s_jump%s_%s_%s.zip" %  (ddw, distance, key, strategy))
					print("building %s started at %s" % (outputfile, dt.utcnow().isoformat()), end=' ')
					builder\
						.setMaxFaultSections(max_sections)\
						.setMaxJumpDistance(distance)\
						.setPermutationStrategy(strategy)\
						.setMaxSubSectionLength(ddw)\
						.buildRuptureSet(filename)
						#.minSubSectsPerParent(2)\

					#report it
					duration = (dt.utcnow() - t0).total_seconds()
					results = test_arguments(builder, key, outputfile, strategy, distance, ddw, max_sections) #record the input args
					results.update(ruptureSetMetrics(builder)) #record the result metrics
					results.update(repoheads) # record the repo refs
					results['datetime'] = dt.utcnow().isoformat()
					results['duration'] = duration					
					writer.writerow(**results)
					
					builder.writeRuptureSet(str(outputfile))
					print("; took %s secs" % (dt.utcnow() - t0).total_seconds())


if __name__ == "__main__":

	gateway = JavaGateway()
	app = gateway.entry_point
	builder = app.getBuilder()

	root_folder = PurePath(os.getcwd())

	##Test parameters
	inputfiles = {
		"ALL": root_folder.joinpath("nshm-nz-opensha/data/FaultModels/DEMO2_DIPFIX_crustal_opensha.xml"),
 		"SANS_TVZ2": root_folder.joinpath("nshm-nz-opensha/data/FaultModels/SANSTVZ2_crustal_opensha.xml")}
	strategies = ['DOWNDIP', 'POINTS', 'UCERF3']
	strategies = ['POINTS',]
	jump_limits = [0.75, 1.0, 2.0, 3.0, 4.0, 4.5, 5.0, 5.1, 5.2, 5.3]
	ddw_ratios = [0.5, 1.0, 1.5, 2.0, 2.5]
	
	#test the tests, nomally 1000 for NZ CFM 
	max_sections = 1000

	repos = ["opensha-ucerf3", "opensha-commons", "opensha-core", "nshm-nz-opensha"]
	repo_root = root_folder #PurePath('/home/chrisbc/DEV/GNS/opensha')
	output_folder = root_folder.joinpath('tmp').joinpath(dt.utcnow().isoformat().replace(':','-'))	
	os.mkdir(output_folder)

	writer = CSVResultWriter(open(output_folder.joinpath('results.csv'), 'w'), repos) 
	repoheads = get_repo_heads(root_folder, repos)
	run_tests(builder, writer, output_folder, repoheads, inputfiles, jump_limits, ddw_ratios, strategies, max_sections)
	print("Done!")

