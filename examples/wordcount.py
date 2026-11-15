"""Word count: the example MapReduce job.

A job file defines two generator functions. The system calls map_fn once for
every line of input, groups everything it yields by key, and calls reduce_fn
once for every key.
"""


def map_fn(key, value):
    """key is the path of the input file, value is one line of text."""
    for word in value.split():
        yield word.lower(), 1


def reduce_fn(key, values):
    """key is a word, values iterates over every count emitted for it."""
    yield key, sum(values)
