import logging


def logger(tag, *keys, default=lambda: None, success="warning"):
    formatter = "%s | %s" % (tag, " | ".join(["%s"]*(len(keys)+1)))

    def select(*args, **kwargs):
        for key in keys:
            if isinstance(key, int):
                try:
                    yield args[key]
                except IndexError:
                    yield None
            else:
                try:
                    yield kwargs[key]
                except KeyError:
                    yield None

    def wrapper(func):

        def wrapped(*args, **kwargs):
            show = list(select(*args, **kwargs))
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                show.append(e)
                logging.error(formatter, *show)
                return default()
            else:
                show.append(result)
                getattr(logging, success)(formatter, *show)
                return result
        return wrapped
    return wrapper