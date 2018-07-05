import datetime
import os
import pickle
import sys

from pyspark.sql import SparkSession

from function import Util
from process.FeatureProcess import feature_process
from robotx.bean import ConfigParser
from robotx.spark import WriteDict

reload(sys)
sys.setdefaultencoding('utf8')


class RobotX(object):
    """
    debug:
    config_path: robotx_config.json
    output_path: taoshu_db_output.rbx_104_RobotXSpark4
    output_type: hive
    output_dict: hdfs://node1:8020/taoshu/engine/work_dir/104/RobotXSpark4/dict.csv
    dict_only: n
    """
    def __init__(self, config_path, output_path, output_type, output_dict, app_name, dict_only=False):
        self.config_path = Util.unix_abs_path(config_path)
        self.dict_only = dict_only
        self.output_path = output_path
        self.output_type = output_type
        self.output_dict = output_dict
        self.app_name = app_name
        self.config_parser = ConfigParser.ConfigParser(config_path)

    def export_dict(self, spark):
        # temporary function,
        # export the property factorList, numList, dateList to a csv
        # (to be used for Atom as dict.csv)
        table = self.config_parser.target
        # output_dict = Util.unix_abs_path('dict.csv',self.output_dict)
        # with open(output_dict, 'w') as fw:
        #     fw.write('variable,type,expression\n')
        #     for x in table.numeric_list:
        #         numeric = x.name
        #         fw.write('"%s",numeric,"%s"\n' %(numeric,x.expression))
        #     for x in table.factor_list:
        #         factor = x.name
        #         fw.write('"%s",factor,"%s"\n' %(factor,x.expression))
        #     for x in table.date_list:
        #         date = x.name
        #         fw.write('"%s",date,"%s"\n' %(date,x.expression))

        sub_sc = spark.sparkContext

        numerics = table.numeric_list
        factors = table.factor_list
        dates = table.date_list
        if self.output_type == 'hive':
            numerics = [field.name for field in numerics]

        # dict_numeric_lst = ['"%s",numeric,"%s"\n' % (x.name, x.expression) for x in table.numeric_list]
        # dict_factor_lst = ['"%s",factor,"%s"\n' % (x.name, x.expression) for x in table.factor_list]
        # dict_date_lst = ['"%s",date,"%s"\n' % (x.name, x.expression) for x in table.date_list]
        # dict_str = 'variable,type,expression\n' \
        #            + ''.join(dict_numeric_lst) \
        #            + ''.join(dict_factor_lst) \
        #            + ''.join(dict_date_lst)
        dict_numeric_lst = ['"%s",numeric\n' % x.name for x in table.numeric_list]
        dict_factor_lst = ['"%s",factor\n' % x.name for x in table.factor_list]
        dict_date_lst = ['"%s",date\n' % x.name for x in table.date_list]
        dict_str = 'variable,type\n' \
                   + ''.join(dict_numeric_lst) \
                   + ''.join(dict_factor_lst) \
                   + ''.join(dict_date_lst)
        WriteDict.write(sub_sc, self.output_dict, dict_str)

    def export_data(self):
        data = self.config_parser.target.data
        # data.show()
        if self.output_type == 'hdfs':
            data.write.csv(self.output_path, header=True, mode='overwrite')
        elif self.output_type == 'hive':
            print "write hive table %s" %self.output_path
            print "partition keys %s"  %str(self.config_parser.target_keys)
            data.write.mode("overwrite").partitionBy(*tuple(self.config_parser.target_keys))\
                .saveAsTable(self.output_path)

    def run(self):
        # self.agent_check()
        t0 = datetime.datetime.now()
        # conf = SparkConf()
        # conf.setAppName(self.app_name)
        # spark_config, master = SparkParser(self.spark_config).config
        # conf.setMaster(master)
        # for k,v in spark_config.items():
        #     conf.set(k,v)
        self.config_parser.parse()
        if self.output_type == "hive" or self.config_parser.hive_support:
            spark = SparkSession \
                .builder \
                .enableHiveSupport() \
                .getOrCreate()
        else:
            spark = SparkSession \
                .builder \
                .getOrCreate()
        # executions:  [<robotx.bean.Beans.Relation object at 0x7f80717c3f90>]
        executions = self.config_parser.execution_sequence
        for relation in executions:
            """
            relation:  <robotx.bean.Beans.Relation object at 0x7f80717c3f90>
            self.dict_only:  False
            self.config_parser:  <robotx.bean.ConfigParser.ConfigParser object at 0x7f8076b1d5d0>
            """
            feature_process(relation, spark, self.dict_only, self.config_parser)
        # export dictionary and data
        # # self.export_dict()
        # from pyspark import SQLContext, SparkConf, SparkContext, SparkFiles
        self.export_dict(spark)
        if not self.dict_only:
            self.export_data()
            # self.agent_update()
        print "total time cost:" + str(datetime.datetime.now() - t0)

    # def agent_check(self):
    #     self.agent_path = Util.unix_abs_path("RobotXSpark.cert", os.path.dirname(os.path.realpath(sys.argv[0])))
    #     if not os.path.isfile(self.agent_path):
    #         raise Exception("the certificate expired or is invalid!")
    #     try:
    #         self.nums, self.date_end = pickle.load(open(self.agent_path, 'rb'))
    #     except Exception as e:
    #         raise Exception("the certificate expired or is invalid!")
    #     if self.nums <= 0 or datetime.datetime.now() > self.date_end:
    #         raise Exception("the certificate expired or is invalid!")
    #
    # def agent_update(self):
    #     self.nums -= 1
    #     pickle.dump((self.nums, self.date_end), open(self.agent_path, 'wb'))
