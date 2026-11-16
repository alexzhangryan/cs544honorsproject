"""Word count with a combiner.

Same job as wordcount.py, plus combine_fn. Each map task then adds up the
counts of a word itself and writes one pair per distinct word, instead of
one pair for every occurrence. (A map task combines in batches of 100,000
pairs, so a large one writes one pair per distinct word per batch.) The
result is the same; there is much less intermediate data to write, read
and group.
"""


def map_fn(key, value):
    """key is the path of the input file, value is one line of text."""
    for word in value.split():
        yield word.lower(), 1


def combine_fn(key, values):
    """Runs inside a map task: key is a word, values are the counts that
    this map task has collected for it so far."""
    yield key, sum(values)


def reduce_fn(key, values):
    """key is a word, values are the partial counts from all map tasks."""
    yield key, sum(values)
