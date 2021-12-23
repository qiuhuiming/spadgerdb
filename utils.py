def default_comparator(comparator: lambda x, y: int) -> lambda x, y: int:
    if comparator is None:
        return lambda x, y: 0 if x == y else -1 if x < y else 1
    return comparator
