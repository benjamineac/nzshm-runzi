#This is the AWS CONTAINER SCRIPT

#export PATH=OPENSHA_JRE :$PATH

export JAVA_CLASSPATH=${NZSHM22_FATJAR}
export CLASSNAME=nz.cri.gns.NZSHM22.opensha.util.NZSHM22_PythonGateway
export NZSHM22_APP_PORT=26533

cd /app/nzshm-runzi

java -Xms4G -Xmx${NZSHM22_SCRIPT_JVM_HEAP_MAX}G -classpath ${JAVA_CLASSPATH} ${CLASSNAME} > ${NZSHM22_SCRIPT_WORK_PATH}/java_app.${NZSHM22_APP_PORT}.log &

python3 -m ${PYTHON_PREP_MODULE} ${TOSHI_FILE_ID}
python3 -m ${PYTHON_TASK_MODULE} ${TASK_CONFIG_JSON_QUOTED} > ${NZSHM22_SCRIPT_WORK_PATH}/python_script.${NZSHM22_APP_PORT}.log

#Kill the Java gateway server
kill -9 $!

#END_OF_SCRIPT


export PATH=/opt/java/openjdk/bin/java:$PATH
export JAVA_CLASSPATH=/app/nzshm-opensha/build/libs/nzshm-opensha-all.jar
export CLASSNAME=nz.cri.gns.NZSHM22.opensha.util.NZSHM22_PythonGateway
export NZSHM22_APP_PORT=26533

cd /app
java -Xms4G -Xmx10G -XX:-UseContainerSupport -XX:ActiveProcessorCount=${NZSHM22_AWS_JAVA_THREADS} -classpath ${JAVA_CLASSPATH} ${CLASSNAME} > /WORKING/java_app.26533.log &
python3 /app/nzshm-runzi/runzi/execute/inversion_diags_report_task.py /WORKING/config.26533.json > /WORKING/python_script.26533.log

#Kill the Java gateway server
kill -9 $!
~
