from pigpy.reports import Report

def filter_report(raw_report, predicate):
    return Report("filtered_%s" % str(raw_report),
        cache_columns=raw_report.cache_columns,
        human_readable_columns=raw_report.human_readable_columns,
        parents={"base_report": raw_report},
        code="%(this)s = FILTER %(base_report)s BY " + predicate + ";"
    )
