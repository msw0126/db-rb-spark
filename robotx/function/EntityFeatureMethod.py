# coding=utf-8
from robotx.bean.Beans import *
from pyspark.sql.types import *
from robotx.function import Util


class EntityMethod(object):

    __metaclass__ = ABCMeta

    def __init__(self, table):
        self.table = table


class OneHot(EntityMethod):
    
    NORMAL_LEVEL_SQL = "(CASE WHEN `{factor}`='{level}' THEN 1 ELSE 0 END) as `{new_name}`"
    OTHER_LEVEL_SQL = "(CASE WHEN `{factor}` in ('{level}') THEN 0 ELSE 1 END) as `{new_name}`"

    def execute(self, spark):
        table = self.table
        field = self.field

        one_hot_fields = list()
        for level,mapping in zip(field.factor_levels,field.factor_levels_mapping):

            new_field_name = field.name+"~"+ mapping
            describer = OneHotDescriber(field, level)
            new_field = NumericField(name=new_field_name, describer=describer)
            new_field.one_hoted()
            new_field.describer = describer
            table+new_field

            if isinstance(level, list):
                one_hot_fields.append(OneHot.OTHER_LEVEL_SQL.format(
                    factor=field.name,
                    level="','".join(level),
                    new_name=new_field_name
                ))
            else:
                one_hot_fields.append(OneHot.NORMAL_LEVEL_SQL.format(
                    factor=field.name,
                    level=level,
                    new_name=new_field_name
                ))

        return one_hot_fields

    def __init__(self, table, field):
        super(OneHot, self).__init__(table)
        self.field = field


class OneHotDescriber(object):

    def __init__(self, field, level):
        self.field = field
        self.level = "OTHER" if isinstance(level, list) else level

    @property
    def expression(self):
        factor = self.field.expression
        level = self.level
        return "if({f}=='{l}',1,0)".format(f=factor, l=level)


class Pivot(EntityMethod):

    NORMAL_LEVEL_SQL = "(CASE WHEN `{factor}`='{level}' THEN `{numeric}` ELSE 0 END) as `{new_name}`"
    OTHER_LEVEL_SQL = "(CASE WHEN `{factor}` in ('{level}') THEN 0 ELSE `{numeric}` END) as `{new_name}`"
    
    def __init__(self, table, factor, numeric):
        super(Pivot, self).__init__(table)
        self.factor = factor
        self.numeric = numeric

    def execute(self, spark):
        """
        :type spark: SparkSession
        :return: 
        """
        table = self.table
        factor = self.factor
        numeric = self.numeric

        pivot_fileds = list()
        #for level in factor.factor_levels:
        for level, mapping in zip(factor.factor_levels, factor.factor_levels_mapping):
            #new_field_name = numeric.name + "*" + factor.name + "~" + ("null" if level is None else level)
            new_field_name = numeric.name + "*" + factor.name + "~" + mapping
            describer = PivotDescriber(numeric, factor, level)
            new_field = NumericField(name=new_field_name, describer=describer)
            new_field.pivoted()
            new_field.describer = describer
            table + new_field

            if isinstance(level, list):
                pivot_fileds.append(Pivot.OTHER_LEVEL_SQL.format(
                    factor=factor.name, level="','".join(level), new_name=new_field_name, numeric=numeric.name
                ))
            else:
                pivot_fileds.append(Pivot.NORMAL_LEVEL_SQL.format(
                    factor=factor.name, level=level, new_name=new_field_name, numeric=numeric.name
                ))

        return pivot_fileds


class PivotDescriber(object):

    def __init__(self, numeric, factor, level):
        self.numeric = numeric
        self.factor = factor
        self.level = "OTHER" if isinstance(level, list) else level

    @property
    def expression(self):
        numeric = self.numeric.expression
        factor = self.factor.expression
        level = self.level
        return "{n}*if({f}=='{l}',1,0)".format(n=numeric,f=factor,l=level)


class TwoOp(EntityMethod):

    OPERATOR = None

    def __init__(self, table, field0, field1):
        super(TwoOp, self).__init__(table)
        self.field0 = field0
        self.field1 = field1

    def execute(self):
        table = self.table
        field0, field1 = self.field0, self.field1
        name0, name1 = field0.name, field1.name

        new_field_name = name0 + self.OPERATOR + name1
        describer = TwoOpDescriber(field0, field1, self.OPERATOR)
        new_field = NumericField(name=new_field_name, describer=describer)
        new_field.describer = describer
        table + new_field

        if self.OPERATOR == '+':
            return "`{field0}`+`{field1}` as `{field0}+{field1}`".format(
                field0 = name0,
                field1 = name1,
            )
        elif self.OPERATOR == '-':
            return "`{field0}`-`{field1}` as `{field0}-{field1}`".format(
                field0=name0,
                field1=name1,
            )
        elif self.OPERATOR == '*':
            return "`{field0}`*`{field1}` as `{field0}*{field1}`".format(
                field0=name0,
                field1=name1,
            )
        elif self.OPERATOR == '/':
            return "`{field0}`*1.0/`{field1}` as `{field0}/{field1}`".format(
                field0=name0,
                field1=name1,
            )


class TwoOpDescriber(object):

    def __init__(self, field0, field1, operator):
        self.field0 = field0
        self.field1 = field1
        self.operator = operator

    @property
    def expression(self):
        numeric0 = self.field0.expression
        numeric1 = self.field1.expression
        return "{n0}{op}{n1}".format(n0=numeric0,op=self.operator,n1=numeric1)


class Add(TwoOp):

    OPERATOR = '+'


class Minus(TwoOp):

    OPERATOR = '-'


class Multiply(TwoOp):

    OPERATOR = '*'


class Divide(TwoOp):

    OPERATOR = '/'
