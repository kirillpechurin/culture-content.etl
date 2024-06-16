from internal.utils import coroutine


@coroutine
def make_pipeline(tasks):
    while data := (yield):
        for task in tasks:
            task.delay(data=data)
