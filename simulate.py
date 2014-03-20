import bisect

""" Simulates the completion time of a set of tasks with the given runtimes, assuming 40 slots. """
def simulate(task_runtimes, num_slots=40):
  # Sorted list of task finish times, measured as the time from when the job started.
  finish_times = []
  # Start num_slots tasks.
  while len(finish_times) < num_slots and len(task_runtimes) > 0:
    bisect.insort_left(finish_times, task_runtimes.pop(0))

  while len(task_runtimes) > 0:
    start_time = finish_times.pop(0)
    finish_time = start_time + task_runtimes.pop(0)
    bisect.insort_left(finish_times, finish_time)
    assert(num_slots == len(finish_times))

  # Job finishes when the last task is done.
  return finish_times[-1]