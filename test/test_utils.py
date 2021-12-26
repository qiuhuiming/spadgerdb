import random


def random_user_str(size: int) -> str:
    return ''.join(chr(ord('a') + random.randint(0, 25)) for _ in range(size))
