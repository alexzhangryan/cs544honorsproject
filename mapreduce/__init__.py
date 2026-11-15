"""A small MapReduce implementation: one boss, several workers, shared storage."""

# The most map tasks, and the most reduce tasks, that one job may have. The
# boss keeps every task in memory, so a mistyped count must not be accepted.
MAX_TASKS = 10_000
