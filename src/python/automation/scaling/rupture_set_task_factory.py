#!python3

"""
Wrapper script that produces a job that can be run either locally or
to a cluster using PBS

The job is responsible for

 - launching the java application (with its gateway service configured)
 - executing the python client script + config that calls the Java application
 - updating the task status, files etc via the toshi_api
 - shutting down the java app

 The job is either a bash script (for local machine) or
 a PBS script for the cluster environment
"""
import os
import json
import scaling.rupture_set_builder_task

class RuptureSetTaskFactory():

    def __init__(self, root_path, working_path, jre_path=None, app_jar_path=None, task_config_path=None, pbs_script=False, initial_gateway_port=25333, pbs_ppn=8, pbs_wall_hours=24):
        """
        pbs_script: boolean is this a PBS job?
        initial_gateway_port: what port to start incrementing from
        """
        self._next_port = initial_gateway_port
        self._pbs_script = pbs_script
        self._pbs_ppn = pbs_ppn #define hows many processors the PBS job should 'see'
        self._pbs_nodes = 1 #always ust one PBS node (and which one we don't know)
        self._pbs_wall_hours = pbs_wall_hours #defines maximum time the jobs is allocated by PBS

        self._jre_path = jre_path or "/opt/sw/java/java-11-openjdk-amd64/bin/java"
        self._app_jar_path = app_jar_path or "~/NSHM/opensha/nshm-nz-opensha/build/libs/nshm-nz-opensha-all.jar"
        self._config_path = task_config_path or os.getcwd()
        self._script_path = os.path.dirname(scaling.rupture_set_builder_task.__file__) #path to the actual task script

        self._root_path = root_path #path containing the git repos
        self._working_path = working_path

        self._jvm_heap_start_gb = 4
        self._jvm_heap_max_gb = 12
        self._python = 'python'


    def write_task_config(self, task_arguments, job_arguments):
        data =dict(task_arguments=task_arguments, job_arguments=job_arguments)
        fname = f"{self._config_path}/config.{self._next_port}.json"
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)


    def get_task_script(self):
        if not self._pbs_script:
            return self._get_bash_script()
        else:
            return self._get_pbs_script()

    def get_next_port(self):
        return self._next_port

    def _get_bash_script(self):
        """
        get the bash for the next task
        """

        script = f"""
export PATH={self._jre_path}:$PATH
export JAVA_CLASSPATH={self._app_jar_path}
export CLASSNAME=nz.cri.gns.NZSHM22.opensha.util.NZSHM22_PythonGateway
export NZSHM22_APP_PORT={self._next_port}

cd {self._root_path}
java -Xms{self._jvm_heap_start_gb}G -Xmx{self._jvm_heap_max_gb}G -classpath ${{JAVA_CLASSPATH}} ${{CLASSNAME}} > {self._working_path}/java_app.{self._next_port}.log &
{self._python} {self._script_path}/rupture_set_builder_task.py {self._config_path}/config.{self._next_port}.json > {self._working_path}/python_script.{self._next_port}.log

#Kill the Java gateway server
kill -9 $!
"""
        self._next_port +=1
        return script


    def _get_pbs_script(self):
        return f"""
#PBS -l nodes={self._pbs_nodes}:ppn={self._pbs_ppn}
#PBS -l walltime={self._pbs_wall_hours}:00:00

{self._get_bash_script()}

#END_OF_PBS
"""

