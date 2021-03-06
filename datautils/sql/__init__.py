

def make_command(name, fields, **filters):
    template = "select %s from %s"
    where = "%s where %s"
    fields = ",".join(fields) if fields else "*"
    command = template % (fields, name)
    if len(filters) == 0:
        return command
    else:
        return where % (command, " and ".join(iter_filters(**filters)))


def iter_filters(**filters):
    for key, value in filters.items():
        if isinstance(value, (set, list)):
            if len(value) > 1:
                yield "%s in %s" % (key, tuple(value))
            elif len(value) == 0:
                yield from iter_filters(**{key: value[0]})
        elif isinstance(value, tuple):
            start, end = value[0], value[1]
            if start:
                yield "%s>=%s" % (key, start)
            if end:
                yield "%s<=%s" % (key, end)
        elif value is not None:
            if isinstance(value, str):
                value = "'%s'" % value
            yield "%s = %s" % (key, value)
        else:
            yield "%s is NULL" % key