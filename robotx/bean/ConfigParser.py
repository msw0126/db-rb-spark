import csv

from robotx.bean.Beans import *
from collections import OrderedDict
from robotx.function.RelationalFeatureMethod import Sum, Avg, Min, Max
from robotx.function.FactorLevelLimitation import FactorLevelLimitation
import json
from robotx.bean import DAG
import os


class ConfigParser(object):
    def __init__(self, config_path):
        """
        :param config_path: str, path of the configuration file (.json format, utf-8 encoding)
        """
        self.config_path = config_path
        self.table_mapping = OrderedDict()
        self.relation_list = list()
        self.limitation = dict()
        self.hive_support = False
        self.execution_sequence = None  # a list of relation, the order in list is the order of table join
        self.target = None  # target table
        self.target_keys = set()

        self.SUMMARY_METHOD_MAPPING = [Avg, Sum]
        self.CROSS = False
        self.PIVOT = False
        self.FL = FactorLevelLimitation(max_level=200, obtain_max_level=20, obtain_percent=0.85)

    def parse(self):
        # config_path: thr path of the configuration file (.json format)
        # set the dictionary and the list, they are:
        # a dictionary of <tableName(str) : tableObject(Table)> and
        # a list of Relation

        config_path = self.config_path

        if not os.path.exists(config_path):
            raise Exception("Configuration file", config_path,
                            "doesn't exist. Please check the config path. ")

        with open(config_path, 'r') as f:
            config_json = json.load(f)

        # use OrderedDict to confirm a reproducible result
        data = OrderedDict(config_json["data"])
        describe = OrderedDict(config_json["describe"])
        relation = config_json["relation"]
        self.parse_limitatiion(config_json)

        # todo need more discussion
        entity_dict = OrderedDict(dict() if "entity" not in config_json else config_json["entity"])

        for table_name, table_describe in data.items():
            delimiter = ","
            if "delimiter" in table_describe:
                delimiter = table_describe["delimiter"]
            table_path = None
            if "path" in table_describe:
                table_path = table_describe['path']
            table_head = None
            if "head" not in table_describe and 'table' not in table_describe:
                # todo if table_path is an hdfs path, should use a different method
                reader = csv.reader(open(table_path, "r"), delimiter=delimiter)
                for row in reader:
                    table_head = row
                    break
            elif "head" in table_describe:
                table_head = table_describe["head"]
            has_header = True
            if "has_header" in table_describe:
                has_header = table_describe["has_header"]
            hive_table_path = None
            if "table" in table_describe:
                hive_table_path = table_describe["table"]
                self.hive_support = True
            table = Table(name=table_name, all_fields=table_head, has_header=has_header, delimiter=delimiter,
                          path=table_path,
                          hive_table_path=hive_table_path)
            self.table_mapping[table_name] = table

        for table_name, value in describe.items():
            numeric = value['numeric'] if 'numeric' in value else list()  # list of str
            factor = value[
                'factor'] if 'factor' in value else list()  # list of dict, in some case, level doesn't exist.
            date = value['date'] if 'date' in value else list()  # list of dict
            if table_name not in self.table_mapping:
                raise Exception("Error:[%s] not found in data" % table_name)

            table = self.table_mapping[table_name]
            for numeric_ in numeric:
                table + NumericField(numeric_, table_name)
            for factor_ in factor:
                factor_name = factor_
                factor_level = None
                if isinstance(factor_, dict):
                    factor_name = factor_['name']
                    if 'level' in factor_:
                        factor_level = factor_['level']
                table + FactorField(factor_name, table_name, factor_levels=factor_level)
            for date_ in date:
                table + DateField(date_['name'], date_['format'], table_name)

        for relation_ in relation:
            source = relation_['source']  # str
            target = relation_['target']  # str
            join_fields = relation_['join']  # list of dict
            relation_type = relation_['type'].upper()  # str
            interval = relation_['interval'] if 'interval' in relation_ else None
            if interval is not None:
                if isinstance(interval, list):
                    interval = interval
                elif isinstance(interval, int):
                    interval = [interval]
                else:
                    raise Exception('interval of relation must be int or list of int.')

            if target not in self.table_mapping:
                raise Exception("Error:[%s] not found in data" % target)
            if source not in self.table_mapping:
                raise Exception("Error:[%s] not found in data" % source)

            target = self.table_mapping[target]
            source = self.table_mapping[source]
            join_fields = [(target[join['tg_field']], source[join['sc_field']]) for join in join_fields]
            relation = Relation(target, source, relation_type, join_fields, interval)
            self.relation_list.append(relation)
        self.execution_path()
        # entity is not used for the moment

    def execution_path(self):
        self.execution_sequence, self.target = DAG.getSortedRelationList(self.relation_list)
        # self.target is the (eventual) target table in relationList
        target_name = self.target.name
        for relation in self.relation_list:
            if relation.target_table.name == target_name:
                for field in relation.join_targets:
                    self.target_keys.add(field)

    def parse_limitatiion(self, config_json):
        if "limitation" not in config_json:
            # using default values
            return
        limitation = config_json["limitation"]
        if "cross" in limitation:
            cross = limitation['cross']
            if isinstance(cross, bool):
                self.CROSS = cross
        if "pivot" in limitation:
            pivot = limitation['pivot']
            if isinstance(pivot, bool):
                self.PIVOT = pivot
        if "summary_method" in limitation:
            summary_method_ = limitation["summary_method"]
            if isinstance(summary_method_, str) or isinstance(summary_method_, unicode):
                if summary_method_ in ["Avg", "Sum", "Max", "Min"]:
                    self.SUMMARY_METHOD_MAPPING = [eval(summary_method_)]
            elif isinstance(summary_method_, list):
                sms = []
                for sm in summary_method_:
                    if isinstance(sm, str) or isinstance(sm, unicode):
                        if sm in ["Avg", "Sum", "Max", "Min"]:
                            sms.append(eval(sm))
                if len(sms) > 0:
                    self.SUMMARY_METHOD_MAPPING = sms
        max_level = 200
        if "max_level" in limitation:
            max_level_ = limitation["max_level"]
            if isinstance(max_level_, int):
                max_level = max_level_
        obtain_max_level = 20
        if "obtain_max_level" in limitation:
            obtain_max_level_ = limitation["obtain_max_level"]
            if isinstance(obtain_max_level_, int):
                obtain_max_level = obtain_max_level_
        obtain_percent = 0.85
        if "obtain_percent" in limitation:
            obtain_percent_ = limitation["obtain_percent"]
            if isinstance(obtain_percent_, float) and 0 <= obtain_percent_ <= 1:
                obtain_percent = obtain_percent_
        self.FL = FactorLevelLimitation(max_level, obtain_max_level, obtain_percent)


if __name__ == "__main__":
    config = ConfigParser("C:\\work\\robotx-pyspark\\tdir\\config.json")
    config.parse()
    print config
