from collections import OrderedDict
from abc import abstractmethod, ABCMeta
from pyspark.sql.functions import *
from robotx.function.Util import *


class Table(object):
    def __init__(self, name, all_fields=None, has_header=True, delimiter=",", path=None,
                 hive_table_path=None):
        self.field_mapping = OrderedDict()
        self.numeric_list = list()  # type: list[NumericField]
        self.factor_list = list()  # type: list[FactorField]
        self.date_list = list()  # type: list[DateField]
        self.all_fields = all_fields
        self.name = name
        self.path = path
        self.data = None  # type: DataFrame
        self.efeated = False
        self.has_header = has_header
        self.delimiter = delimiter
        self.hive_table_path = hive_table_path

    def __add__(self, other):
        self.field_mapping[other.name] = other
        if isinstance(other, NumericField):
            self.numeric_list.append(other)
        elif isinstance(other, FactorField):
            self.factor_list.append(other)
        elif isinstance(other, DateField):
            self.date_list.append(other)
        return self

    def __sub__(self, other):
        if isinstance(other, str):
            self.field_mapping.pop(other)
        elif isinstance(other, Field):
            self.field_mapping.pop(other.name)
        return self

    def __contains__(self, item):
        return item in self.field_mapping

    def __call__(self):
        return self.field_mapping.values()

    def __getitem__(self, item):
        return self.field_mapping[item]

    def efeated(self):
        self.efeated = True

    @property
    def numeric(self):
        return self.numeric_list

    @property
    def numeric_ori(self):
        return [field for field in self.numeric_list
                if not field.one_hot
                and not field.pivot
                and not field.join]

    @property
    def factor(self):
        return self.factor_list

    @property
    def factor_ori(self):
        return [field for field in self.factor_list
                if not field.one_hot
                and not field.pivot
                and not field.join]

    @property
    def date(self):
        return self.date_list

    @property
    def fields(self):
        fields = list()
        fields.extend(self.date_list)
        fields.extend(self.factor_list)
        fields.extend(self.numeric_list)
        return fields


    def clone(self):
        table = Table(self.name, self.all_fields, self.path)
        table.field_mapping = self.field_mapping
        table.numeric_list = self.numeric_list
        table.factor_list = self.factor_list
        table.date_list = self.date_list
        table.efeated = self.efeated
        return table


class Field(object):
    __metaclass__ = ABCMeta

    def __init__(self, name, table=None, describer=None):
        self.table = table
        self.name = name
        self.describer = describer
        self.one_hot = False
        self.pivot = False
        self.join = False

    @property
    def expression(self):
        if self.describer is None:
            return "%s[%s]" %(self.table,self.name)
        else:
            return self.describer.expression

    def one_hoted(self):
        self.one_hot = True

    def pivoted(self):
        self.pivot = True

    def joined(self):
        self.join = True

    @abstractmethod
    def field_type(self): pass


class NumericField(Field):
    @property
    def field_type(self):
        return 'numeric'

    def __init__(self, name, table=None, describer=None):
        super(NumericField, self).__init__(name, table, describer)


class FactorField(Field):
    @property
    def field_type(self):
        return 'factor'

    def __init__(self, name, table=None, describer=None, factor_levels=None):
        super(FactorField, self).__init__(name, table, describer)
        self.factor_levels = None
        self.factor_levels_mapping = None
        if factor_levels is not None:
            self.init_factor_levels(factor_levels)

    def init_factor_levels(self, factor_levels):
        self.factor_levels = factor_levels
        self.factor_levels_mapping = ['u'+str(i) for i in range(len(factor_levels))]


class DateField(Field):

    @property
    def field_type(self):
        return 'date'

    def __init__(self, name, date_format, table=None, describer=None):
        super(DateField, self).__init__(name, table, describer)
        self.date_format = date_format

    def __sub__(self, other):
        """
        e.g.
        round((UNIX_TIMESTAMP(a, yyyyMMdd hh:mm:ss)-UNIX_TIMESTAMP(b, yyyyMMdd hh:mm:ss))/1)
        round((UNIX_TIMESTAMP(a, yyyyMMdd hh:mm)-UNIX_TIMESTAMP(b, yyyyMMdd hh:mm:ss))/60)
        round((UNIX_TIMESTAMP(a, yyyyMMdd hh)-UNIX_TIMESTAMP(b, yyyyMMdd hh:mm:ss))/3600)
        months_between(FROM_UNIXTIME(UNIX_TIMESTAMP(a,yyyyMM)),FROM_UNIXTIME(UNIX_TIMESTAMP(b,yyyyMMdd hh:mm:ss)))
        year(FROM_UNIXTIME(UNIX_TIMESTAMP(a,yyyy)))-year(FROM_UNIXTIME(UNIX_TIMESTAMP(b,yyyyMMdd hh:mm:ss)))
        :param other: another DateField object
        :return: sql expression
        """


        s_dateformat = self.date_format
        t_dateformat = other.date_format
        s_name = self.name
        t_name = other.name
        sql = sqlExp(s_dateformat, t_dateformat, s_name, t_name)
        return sql


class Relation(object):
    BACKWARD = 'BACKWARD'
    FORWARD = 'FORWARD'

    def __init__(self, target_table, source_table, relation_type, join_fields, interval=None):
        """
        :type target_table: Table
        :type source_table: Table
        :type relation_type: str
        :type join_fields: list[tuple[Field,Field]]
        :type interval: list[int]
        """
        self.target_table = target_table
        self.source_table = source_table
        self.relation_type = relation_type.upper()
        self.join_fields = join_fields
        self.interval = interval
        self.join_date = None
        for join_target, join_source in join_fields:
            if isinstance(join_target, DateField):
                self.join_date = (join_target, join_source)

    def __contains__(self, item):
        for join_target, join_source in self.join_fields:
            if isinstance(item, unicode) and item == join_source.name:
                return True
            if isinstance(item, Field) and item.name == join_source.name:
                return True
        return False

    @property
    def forward(self):
        return self.relation_type == Relation.FORWARD

    @property
    def has_date(self):
        return self.join_date is not None

    @property
    def join_sources(self):
        return [join_source.name for join_target, join_source in self.join_fields]

    @property
    def join_targets(self):
        return [join_target.name for join_target, join_source in self.join_fields]


if __name__ == "__main__":
    pass
