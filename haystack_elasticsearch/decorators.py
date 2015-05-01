from __future__ import unicode_literals

import functools


class AutoBuild(object):
    """Decorator that makes a class autobuild when method is called.

    :param func: Function to decorate.
    """

    def __init__(self, func, *args, **kwargs):
        self.func = func
        functools.update_wrapper(self, func)

    def __call__(self, *args, **kwargs):
        if not args[0]._built:
            args[0].build()

        return self.func(*args, **kwargs)

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self.func
        return functools.partial(self, obj)


def safe_prepare(default):
    """Wrapper for SafePrepare that provides the next syntax: @safe_prepare(default_value)

    :param default: Default returning value.
    """
    return functools.partial(SafePrepare, default=default)


class SafePrepare(object):
    """Decorator that makes safe the prepare method of a field, returning a
    default value when a exception is raised.

    :param func: Function to be decorated.
    :param default: Default returning value.
    """
    def __init__(self, func, default=None):
        self.func = func
        self.default = default
        functools.update_wrapper(self, func)

    def __call__(self, *args, **kwargs):
        try:
            return self.func(*args, **kwargs)
        except:
            return self.default

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self.func
        return functools.partial(self, obj)