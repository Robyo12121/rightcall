import string
import random


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    """Generates a pseudorandom string of the specefied length using the
        specefied character sets
    INPUT:
        size: <int> length of output string
        chars: <string.character_set> characterset from which to generate output string"""
    return ''.join(random.choice(chars) for _ in range(size))


if __name__ == '__main__':
    for i in range(20):
        my_id = id_generator(size=10, chars=['a', 'b', 'c'])
        print(my_id)
