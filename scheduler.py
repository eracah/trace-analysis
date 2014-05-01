from bisect import insort_left, bisect_right
from parse_logs import AbstractTask

def schedule(abstract_tasks, remove_fn):
    """
    return a list of abstract_tasks with the appropriate times removed and rescheduled

    Use the original start/end times to infer dependencies
    """
    map_end_original = sorted([x for x in abstract_tasks if not x.is_reduce], key=lambda x : x.stop)[-1]
    abstract_tasks = replace_shuffles(abstract_tasks, map_end_original.stop)
    assert(map_end_original in abstract_tasks)

    start = abstract_tasks[0].start
    sorted_end_times = sorted([task.stop for task in abstract_tasks])
    dependencies = {task: bisect_right(sorted_end_times, task.start) - 1 for task in abstract_tasks}

    new_schedule = []
    for task in abstract_tasks:
        dep = dependencies[task]
        current = task
        if dep == -1:
            start_time = start
        else:
            start_time = max(x.stop for x in sorted(new_schedule, key=lambda x: x.stop)[:dependencies[task] + 1])

        current = remove_fn(current, start_time)#.without(to_remove, start_time)
        new_schedule.append(current)
        if task == map_end_original:
            new_schedule = _fix_reduces(new_schedule, current.stop)
    return new_schedule

def _fix_reduces(schedule, stop_time):
    def fix(task):
        if task.is_reduce:
            return task.reduce_resource('FAKE_SHUFFLE', stop_time - task.start)
        else:
            return task

    return [fix(task) for task in schedule]

def replace_shuffles(tasks, end):
    def replace(task):
        if task.start < end and task.is_reduce:
            return task.switch('SHUFFLE', 'FAKE_SHUFFLE')
        else:
            return task
    return [replace(t) for t in tasks]

def schedule_simple(abstract_tasks, remove_fn, num_slots=23):
    map_end_original = sorted([x for x in abstract_tasks if not x.is_reduce], key=lambda x : x.stop)[-1]
    abstract_tasks = replace_shuffles(abstract_tasks, map_end_original.stop)

    start = abstract_tasks[0].start
    sorted_end_times = [start for _ in xrange(num_slots)]

    new_schedule = []
    for task in abstract_tasks:
        start_time = sorted_end_times.pop(0)
        current = remove_fn(task, start_time)#.without(to_remove, start_time)
        insort_left(sorted_end_times, current.stop)
        new_schedule.append(current)
        if task == map_end_original:
                new_schedule = _fix_reduces(new_schedule, current.stop)
    return new_schedule