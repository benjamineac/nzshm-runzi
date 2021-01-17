import json
import git
import csv
import os
from pathlib import PurePath
from py4j.java_gateway import JavaGateway
import datetime as dt
from dateutil.tz import tzutc


if __name__ == "__main__":

    #setup the java gateway binding
    gateway = JavaGateway()
    app = gateway.entry_point
    #builder = app.getBuilder()
    inversion_runner = app.getRunner()
    hazard_calc = app.getCalculator()

    ##Test parameters
    # inputfile = "/home/chrisbc/DEV/GNS/opensha/tmp/2020-11-22T23-49-54.671294/ruptset_ddw0.5_jump5.0_SANS_TVZ2_560.0_2_DOWNDIP.zip"
    # inputfile = "/home/chrisbc/DEV/GNS/opensha/tmp/2020-12-07T01-14-52.776388/ruptset_ddw0.5_jump5.0_SANS_TVZ2_580.0_2_DOWNDIP.zip"
    #inputfile = "/home/chrisbc/DEV/GNS/opensha/tmp/2020-12-07T23-33-01.777193/ruptset_ddw0.5_jump5.0_SANS_TVZ2_580.0_2_DOWNDIP_0.1.zip"
    #inputfile = "/home/chrisbc/DEV/GNS/opensha/tmp/2020-12-07T23-40-04.296119/ruptset_ddw0.5_jump5.0_SANS_TVZ2_580.0_2_DOWNDIP_thin0.5.zip"
    #inputfile = "/home/chrisbc/DEV/GNS/opensha/tmp/2020-12-14T00-03-34.026887/ruptset_ddw0.5_jump5.0_SANS_TVZ2_HIKURANGI_1_580.0_2_UCERF3_thin0.1.zip"
    #inputfile = "/home/chrisbc/DEV/GNS/opensha/tmp/2020-12-15T00-02-59.733572/ruptset_ddw0.5_jump5.0_SANS_TVZ2_HIKURANGI_1_580.0_2_UCERF3_thin0.1.zip"

    # COMBO 330K
    inputfile = "/home/chrisbc/DEV/GNS/opensha/tmp/2020-12-15T02-58-03.807233/ruptset_ddw0.5_jump5.0_SANS_TVZ2_HIKURANGI_1_580.0_2_UCERF3_thin0.1.zip"

    t0 = dt.datetime.utcnow()
    INVERSION_MINS = 2
    SOLUTION_FILE = "/home/chrisbc/DEV/GNS/opensha/tmp/reports/TestSolution_%sm_COMBINED_330K.zip" % INVERSION_MINS
    TOTAL_RATE_M5 = 8.8

    print("Starting inversion of %s minutes" % INVERSION_MINS)
    print("=================================")
    inversion_runner\
        .setInversionMinutes(INVERSION_MINS)\
        .setSyncInterval(30)\
        .setRuptureSetFile(inputfile)\
        .setGutenbergRichterMFD(TOTAL_RATE_M5,
            1.0 , #bValue=
            7.85, #mfdTransitionMag=
            40, #mfdNum=
            5.05, # mfdMin=
            8.95 #mfdMax=
            )\
        .runInversion()
    inversion_runner.writeSolution(SOLUTION_FILE)

    t1 = dt.datetime.utcnow()
    print("Inversion took %s secs" % (t1-t0).total_seconds())

    # print("Setting up hazard")
    # print("=================")
    # calc = hazard_calc\
    #     .setForecastTimespan(50.0)\
    #     .setSolutionFile(SOLUTION_FILE)\
    #     .setMaxDistance(250.0)\
    #     .build()

    # t2 = dt.datetime.utcnow()
    # print("took %s secs" % (t2-t1).total_seconds())

    # print("Hazard in Site...")
    # print("==========================")
    # masterton = dict(lat=-40.95972, lon=175.6575)
    # wellington = dict(lat=-41.289, lon=174.777)
    # result = calc.calc(wellington['lat'], wellington['lon'])

    # # print(dir(result))
    # """
    # ['areAllXValuesInteger', 'calcSumOfY_Vals', 'clear', 'deepClone', 'equals', 'forEach', 'fromXMLMetadata',
    # 'get', 'getClass', 'getClosestXtoY', 'getClosestYtoX', 'getDatasetsToPlot',
    # 'getFirstInterpolatedX', 'getFirstInterpolatedX_inLogXLogYDomain', 'getFirstInterpolatedX_inLogYDomain',
    # 'getIndex', 'getInfo', 'getInterpExterpY_inLogYDomain', 'getInterpolatedY', 'getInterpolatedY_inLogXDomain',
    # 'getInterpolatedY_inLogXLogYDomain', 'getInterpolatedY_inLogYDomain', 'getMaxX', 'getMaxY',
    # 'getMetadataString', 'getMinX', 'getMinY', 'getName', 'getPlotNumColorList',
    # 'getPointsIterator', 'getTolerance', 'getX', 'getXAxisName', 'getXIndex', 'getXVals',
    # 'getXValuesIterator', 'getY', 'getYAxisName', 'getYVals', 'getYValuesIterator',
    # 'getYY_Function', 'hasX', 'hashCode', 'iterator', 'loadFuncFromSimpleFile',
    # 'main', 'notify', 'notifyAll', 'scale', 'set', 'setInfo', 'setName',
    # 'setTolerance', 'setXAxisName', 'setYAxisName', 'size', 'spliterator', 'toDebugString',
    # 'toString', 'toXMLMetadata', 'wait', 'writeSimpleFuncFile', 'xValues', 'yValues']
    # """

    # fout = open("wgtn_50yr_250km_PGA_inversion_for_%sm_COMBINED_330K." % INVERSION_MINS, 'w')
    # fout.write(result.getInfo())
    # fout.write('\n\n')
    # fout.write(result.toString())

    # t3 = dt.datetime.utcnow()
    # print("took %s secs" % (t3-t2).total_seconds())
    print("Done!")
