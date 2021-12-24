def random_user_str(size: int) -> str:
    return ''.join(chr(ord('a') + i) for i in range(size))
