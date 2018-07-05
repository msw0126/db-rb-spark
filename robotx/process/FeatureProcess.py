from robotx.data.DataLoad import *
from robotx.function.EntityFeatureMethod import *
from robotx.function.RelationalFeatureMethod import *
from copy import deepcopy
import robotx.bean.Constant as C
from robotx.bean.ConfigParser import ConfigParser


def __join_table__(source, target, relation, spark, dict_only):
    CsvLoader(target).load(spark)
    source_data = source.data
    field_exp = list()
    for field in source.fields:
        field_exp.append("`{1}` as `${0}[{1}]`".format(source.name, field.name))
        # join field in target table describe
        if field.name not in relation:
            new_field = deepcopy(source[field.name])
            # rename
            new_field.name = "${0}[{1}]".format(source.name, field.name)
            # tag it
            new_field.joined()
            target + new_field

    source_data = source_data.selectExpr(*tuple(field_exp))
    if not dict_only:
        source_data.persist()
    # join source and target
    join_cond = list()
    join_source_fields = list()
    for join_target, join_source in relation.join_fields:
        source_field_name = "${0}[{1}]".format(source.name, join_source.name)
        join_source_fields.append(source_field_name)
        join_cond.append(target.data[join_target.name] == source_data[source_field_name])
    target.data = target.data.join(source_data, join_cond, how="left")
    target.data = target.data.drop(*tuple(join_source_fields))


def __entity_feature__(source, target, relation, spark, dict_only, config_parser):
    CsvLoader(source).load(spark)

    ori_numeric = [numeric for numeric in source.numeric_ori if numeric not in relation]

    # cross between numeric
    if config_parser.CROSS:
        field_exp = list("*")
        for idx0 in range(len(ori_numeric) - 1):
            numeric0 = ori_numeric[idx0]
            for idx1 in range(idx0 + 1, len(ori_numeric)):
                numeric1 = ori_numeric[idx1]
                field_exp.append(Add(source, numeric0, numeric1).execute())
                field_exp.append(Minus(source, numeric0, numeric1).execute())
                field_exp.append(Divide(source, numeric0, numeric1).execute())
                field_exp.append(Divide(source, numeric1, numeric0).execute())
        # persist
        source.data = source.data.selectExpr(*tuple(field_exp))
    __join_table__(source, target, relation, spark, dict_only)


def __relational_feature__(source, target, relation, spark, dict_only, config_parser):
    # source:Table, target:Table, relation: Relation, spark: SparkSession
    # whether have date or not
    if relation.has_date:
        __relational_with_date__(source, target, relation, spark, dict_only, config_parser)
    else:
        __relational_without_date__(source, target, relation, spark, dict_only, config_parser)


def __relational_with_date__(source, target, relation, spark, dict_only, config_parser):
    """
    :type source: Table 
    :type target: Table 
    :type relation: Relation 
    :type spark: SparkSession
    :return: 
    """
    date_target, date_source = relation.join_date
    date_target_name = "${0}[{1}]".format(target.name, date_target.name)

    CsvLoader(target).load(spark)
    # join two table
    # take specific cols of target
    slice_exp = list()
    for join in relation.join_targets:
        slice_exp.append("`{1}` as `${0}[{1}]`".format(target.name, join))
    target_data_slice = target.data.selectExpr(*tuple(slice_exp))

    # used in group by
    join_cond = list()
    # used in group interval
    group_interval = list()
    # should be dropped after join
    join_target_list = list()
    for join_target, join_source in relation.join_fields:
        if isinstance(join_target, DateField):
            continue
        target_field_name = "${0}[{1}]".format(target.name, join_target.name)
        join_target_list.append(target_field_name)
        join_cond.append(source.data[join_source.name] == target_data_slice[target_field_name])
        group_interval.append(join_source.name)
    source_data = source.data.join(target_data_slice, on=join_cond, how="inner")

    # calculate the difference between two date
    date_diff_reconstruct_exp = list()
    for field in source.fields:
        if field.name == date_source.name:
            continue
        date_diff_reconstruct_exp.append("`%s`" % field.name)
    date_diff_reconstruct_exp.append("`%s` as `%s`" % (date_target_name, date_source.name))
    date_diff_reconstruct_exp.append("(%s) as `%s`"
                                     % (DateField(date_target_name, date_target.date_format) - date_source,
                                        C.__DATE_DIFF_COL__))

    source_data = source_data.selectExpr(*tuple(date_diff_reconstruct_exp))
    # filter according to date
    source_data = source_data.filter(source_data[C.__DATE_DIFF_COL__] > 0)

    # relational feature without date
    source_for_no_date = deepcopy(source.clone())
    source_for_no_date.data = source_data
    join_fields_for_no_date = [(join_target, join_source) for join_target, join_source in relation.join_fields
                               if not isinstance(join_target, DateField)]
    relation_for_no_date = Relation(target, source_for_no_date, Relation.BACKWARD, join_fields_for_no_date)
    __relational_without_date__(source_for_no_date, target, relation_for_no_date, spark, dict_only, config_parser)

    group_interval.append(date_source.name)
    for interval in relation.interval:
        date_diff_col = C.__DATE_DIFF_COL__ + str(interval)
        source_data_interval = source_data.withColumn(date_diff_col,
                                                      (((col(C.__DATE_DIFF_COL__) - 1) / interval) + 1).cast(
                                                          IntegerType()))
        group_interval_ = deepcopy(group_interval)
        group_interval_.append(date_diff_col)
        group_data = source_data_interval.groupBy(group_interval_)

        table = Table(source.name)
        for field in relation.join_sources:
            table + source[field]

        # aggregate
        agg_list = list()
        cross_dict = {method_.__name__: list() for method_ in config_parser.SUMMARY_METHOD_MAPPING}
        for field in source.numeric_list:
            if field in relation:
                continue
            for method_ in config_parser.SUMMARY_METHOD_MAPPING:
                method_name = method_.__name__
                if method_.ONE_HOT_EXCEPTED and field.one_hot:
                    # method is not suitable for one hot field
                    continue
                method_ = method_(source, field, table, interval)
                agg_list.append(method_.method())
                if field.join or field.pivot or field.one_hot:
                    continue
                cross_dict[method_name].append(method_.new_field)

        summary_source_interval = group_data.agg(*tuple(agg_list))
        table.data = summary_source_interval

        # cross
        if config_parser.CROSS:
            field_exp = list("*")
            for fields in cross_dict.values():
                for idx0 in range(len(fields) - 1):
                    numeric0 = fields[idx0]
                    for idx1 in range(idx0 + 1, len(fields)):
                        numeric1 = fields[idx1]
                        field_exp.append(Add(table, numeric0, numeric1).execute())
                        field_exp.append(Minus(table, numeric0, numeric1).execute())
                        field_exp.append(Divide(table, numeric0, numeric1).execute())
                        field_exp.append(Divide(table, numeric1, numeric0).execute())
            if len(field_exp) > 1:
                table.data = table.data.selectExpr(*tuple(field_exp))

        # fill na, because in collect_list function, na will be ignored
        na_fill = [numeric.name for numeric in table.numeric_list
                   if numeric not in relation]
        table.data = table.data.na.fill(0.0, na_fill)

        agg_list = [collect_list(col(date_diff_col)).alias(C.__DATE_DIFF_LIST__)]
        for field in table.numeric_list:
            if field in relation:
                continue
            agg_list.append(collect_list(col(field.name)).alias(field.name + C.__LIST_APPENDER__))
        agg_data = table.data.groupBy(*tuple(group_interval)).agg(*tuple(agg_list))

        # construct new table
        agg_table = Table(source.name)
        field_exp = list()
        for field in relation.join_sources:
            agg_table + source[field]
            field_exp.append(source[field].name)

        # time series feature extraction
        agg_table.data = agg_data
        for numeric in table.numeric_list:
            if numeric in relation:
                continue
            field_exp.append(Wma(agg_table, numeric, interval).method(spark))
            field_exp.append(RecentDivideMean(agg_table, numeric, interval).method(spark))
            field_exp.append(Wdiff(agg_table, numeric, interval).method(spark))
            for i in range(3):
                field_exp.append(Recent(agg_table, numeric, interval, i + 1).method(spark))

        agg_table.data = agg_table.data.selectExpr(*tuple(field_exp))
        __join_table__(agg_table, target, relation, spark, dict_only)


def __relational_without_date__(source, target, relation, spark, dict_only, config_parser):
    # source:Table, target:Table, relation: Relation, spark: SparkSession
    # group by relation field
    join_sources = relation.join_sources
    source_data = source.data
    group_source = source_data.groupBy(join_sources)

    table = Table(source.name)
    for field in relation.join_sources:
        table + source[field]

    # aggregate
    agg_list = list()
    cross_dict = {method_.__name__: list() for method_ in config_parser.SUMMARY_METHOD_MAPPING}
    for field in source.numeric_list:
        if field in relation:
            continue
        for method_ in config_parser.SUMMARY_METHOD_MAPPING:
            method_name = method_.__name__
            if method_.ONE_HOT_EXCEPTED and field.one_hot:
                # method is not suitable for one hot field
                continue
            method_ = method_(source, field, table)
            agg_list.append(method_.method())
            if field.join or field.pivot or field.one_hot:
                continue
            cross_dict[method_name].append(method_.new_field)

    summary_source = group_source.agg(*tuple(agg_list))
    table.data = summary_source

    # cross
    if config_parser.CROSS:
        field_exp = list("*")
        for fields in cross_dict.values():
            for idx0 in range(len(fields) - 1):
                numeric0 = fields[idx0]
                for idx1 in range(idx0 + 1, len(fields)):
                    numeric1 = fields[idx1]
                    field_exp.append(Add(table, numeric0, numeric1).execute())
                    field_exp.append(Minus(table, numeric0, numeric1).execute())
                    field_exp.append(Divide(table, numeric0, numeric1).execute())
                    field_exp.append(Divide(table, numeric1, numeric0).execute())
        if len(field_exp) > 1:
            table.data = table.data.selectExpr(*tuple(field_exp))
    __join_table__(table, target, relation, spark, dict_only)


def feature_process(relation, spark, dict_only, config_parser):
    """
    :type relation: Relation
    :type spark: sparkSession
    :type config_parser: ConfigParser
    :return: 
    """
    import datetime
    t1 = datetime.datetime.now()
    source = relation.source_table
    target = relation.target_table
    print source.name, "-->", target.name, "..."

    CsvLoader(source).load(spark)
    field_exp = list("*")
    # todo entity method defined by calculator in web page
    data = source.data

    if not relation.forward:
        # if entity feature do not do one hot and relational feature
        # todo in multi level relation, may have problem, A--entity->B---rfeat--->C    the factor A will never get onehoted or pivoted
        ori_numeric = [numeric for numeric in source.numeric_ori if numeric not in relation]
        ori_factor = [factor for factor in source.factor_ori if factor not in relation]

        if len(ori_factor) > 0:
            n_rows = data.count()
        for factor in ori_factor:
            if factor.factor_levels is None:
                # initialize factor level
                fn = factor.name
                factor_levels = config_parser.FL.execute(data, fn, n_rows)
                # skip factor
                if factor_levels is None:
                    continue
                factor.init_factor_levels(factor_levels)
            # doing one_hot,pivot
            if config_parser.PIVOT:
                for numeric in ori_numeric:
                    field_exp.extend(Pivot(source, factor, numeric).execute(spark))
            field_exp.extend(OneHot(source, factor).execute(spark))
        source.data = source.data.selectExpr(*tuple(field_exp))

    if relation.forward:
        __entity_feature__(source, target, relation, spark, dict_only, config_parser)
    else:
        __relational_feature__(source, target, relation, spark, dict_only, config_parser)
    print source.name, "-->", target.name, "!", str(datetime.datetime.now() - t1)
