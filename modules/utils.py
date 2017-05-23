# encoding: utf-8

""" GROUP QUERIES
""" 
def srange(start, stop, step):
    """ Like range(), but with custom incrementation
    """
    r = start
    while r < stop:
        yield r
        r += step

def get_basepoint(ll, chunk_size):
    """ Get the index of the basepoint, that is the last
        datapoint that fits in a single query 
    """
    prev = 1
    for i, l in enumerate(ll):
        if prev * len(l) > chunk_size:
            j = int(float(chunk_size) / float(prev))
            return (i, j)
        prev = prev * len(l)
    return (len(ll), 0)
