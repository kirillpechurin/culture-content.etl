import functools


def coroutine(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn

    return wrapper
