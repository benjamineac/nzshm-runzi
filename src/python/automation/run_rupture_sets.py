from py4j.java_gateway import JavaGateway
from datetime import datetime as dt
import json
gateway = JavaGateway()
app = gateway.entry_point

builder = app.getBuilder()

inputs = {"ALL": "./data/FaultModels/DEMO2_DIPFIX_crustal_opensha.xml",
	"SANS_TVZ2": "./data/FaultModels/SANSTVZ2_crustal_opensha.xml"}
strategies = ['DOWNDIP', 'POINTS', 'UCERF3']
jump_limits = [0.5, 0.75, 1.0, 2.0, 3.0, 4.0, 5.0, 5.1, 5.2]
ddw_ratios = [0.5, 1.0, 1.5, 2.0, 2.5]
max_sections = 100

def ruptureSetMetrics(builder):		
	conf = builder.getPlausabilityConfig()
	
	metrics = {}
	metrics["sub_section_count"] = builder.getSubSections().size()
	metrics["rupture_count"] = builder.getRuptures().size()
	metrics["possible_cluster_connections_count"] = conf.getConnectionStrategy().getClusterConnectionCount()
	
	# get info form the configuratiion
	conf_diags = json.loads(conf.toJSON())
	conns = 0
	for cluster in conf_diags['connectionStrategy']['clusters']: 
		conns += len(cluster.get('connections',[]))
	metrics["cluster_connections_count"] = conns
	
	return metrics

def run_tests(inputs, jump_limits, ddw_ratios):
    for key, filename in inputs.items():
        for strategy in strategies:
            for distance in jump_limits:
                for ddw in ddw_ratios:
                    t0 = dt.utcnow()
                    print("building /tmp/TEST_ruptureset_ddw%s_jump%s_%s_%s.zip" % (ddw, distance, key, strategy))
                    builder.setMaxFaultSections(max_sections)\
                        .setMaxJumpDistance(distance)\
                        .setPermutationStrategy(strategy)\
                        .setMaxSubSectionLength(ddw)\
                        .buildRuptureSet(filename)
                    print(ruptureSetMetrics(builder))
                    builder.writeRuptureSet("/tmp/TEST_ruptureset_ddw(%s)_jump(%s)_%s_%s.zip" % (ddw, distance, key, strategy))
                    print("built & saved in %s" % str(dt.utcnow() - t0))

if __name__ == "__main__":
    run_tests(inputs, jump_limits, ddw_ratios)
    print("Done!")

