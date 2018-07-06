# -*- coding:utf-8 -*- 
from abc import ABCMeta, abstractmethod
from pyspark.sql.types import *
from robotx.bean.Beans import Table, DateField
from robotx.data import LocalFileLoder

SPARK_DT_TYPE_MAPPING = dict(
    numeric=DoubleType,
    date=StringType,
    factor=StringType
)

SQL_DT_TYPE_MAPPING = dict(
    numeric="double",
    date="string",
    factor="string"
)


class DataLoader(object):

    __metaclass__ = ABCMeta

    def __init__(self, table):
        self.table = table

    @abstractmethod
    def load(self, sc):
        pass


class CsvLoader(DataLoader):

    def __init__(self, table):
        super(CsvLoader, self).__init__(table)

    def load(self, spark):
        # self.table:  <robotx.bean.Beans.Table object at 0x7f204049bf90>
        table = self.table
        if table.data is not None:
            return
        # table.hive_table_path:  taoshu_db_input.user_info 最外层是for循环，读取两个表的信息
        # table.hive_table_path:  taoshu_db_input.overdue
        if table.hive_table_path is not None:
            # 读取hive
            sel_lst = list()
            for field in table():
                dt_type = SQL_DT_TYPE_MAPPING[field.field_type]
                if isinstance(field, DateField):
                    sel_lst.append(
                        '''
                        (case when 
                            from_unixtime(unix_timestamp(`{0}`), '{1}') is null 
                         then `{0}` 
                         else 
                            from_unixtime(unix_timestamp(`{0}`), '{1}') 
                         end) as `{0}`
                        '''.format(field.name, field.date_format))
                else:
                    sel_lst.append("cast(`{0}` as {1}) as `{0}`".format(field.name, dt_type))
            data = spark.sql("select %s from %s" % (",".join(sel_lst), table.hive_table_path))
            # print "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"
            # print "spark.sql\(\"select %s from %s\" % \(\",\".join(sel_lst), table.hive_table_path\)\)",  "select %s from %s" % (",".join(sel_lst), table.hive_table_path)
            # print "data.show()", data.show()
            # print "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"
            table.data = data
        else:
            if table.path.startswith("hdfs"):
                # 读取HDFS csv
                data_schema_list = list()
                for field_name in table.all_fields:
                    if field_name in table:
                        # 该特征在需要处理的字段中，需要设置其数据类型
                        field = table[field_name]
                        field_schema = StructField(field_name, SPARK_DT_TYPE_MAPPING[field.field_type]())
                        data_schema_list.append(field_schema)
                    else:
                        # 改特征不需要处理的字段中，统一设置为StringType
                        field_schema = StructField(field_name, StringType())
                        data_schema_list.append(field_schema)

                data_schema = StructType(data_schema_list)
                reader = spark.read
                data = reader.csv(path=table.path, sep=table.delimiter, schema=data_schema, header=table.has_header,
                                  mode='FAILFAST')
            else:
                # 读取spark-submit 通过 --files上传的数据
                data = LocalFileLoder.load_csv(spark, table)
            field_in_table = ["`%s`" % field_name for field_name in table.all_fields if field_name in table]
            if len(field_in_table) != len(table.all_fields):
                data = data.selectExpr(*tuple(field_in_table))
            table.data = data
