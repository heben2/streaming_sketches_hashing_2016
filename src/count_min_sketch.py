#!/bin/env python3

#import requests
from math import * #ceil, floor, log
from hashing import *
from stream import *
# from stream import parse_to_numeric

#wrapper parser function for input chars.
def parse(a):
    return parse_token(a)

class CountMinSketch:
    '''
    Cash register sketch (allowing c > 0 only).

    Algorithm:
    init:
    c[1..t][1..k] <- 0, k = 2/epsilon, t = ceil(log(1/delta))
    choose t independent 2-universal hash funcs h_1..h_t: [n] -> [k]

    process(j,c):
    for i = 1 to t do:
      C[i][h_i(j)] <- C[i][h_i(j)] + c

    output:
    on query a, report f^_a = min_{1<= i <= t} C[i][h_i(a)]}
    '''

    #N = source universe size
    #k = target universe size, based on accuracy parameter epsilon
    #d = error probability parameter delta.
    #t = num hash functions & inner size of C & num iterations in sketch.
    #    based on d. Default is t = ceil(log(1/d))
    def __init__(self,universe_size,k,d=0.1,t=0):
        print("Initializing count-min or count-median sketch.")
        self.t = int(t)
        if t == 0:
            self.t = int(ceil(log(1/d,2)))
        self.k = int(k)

        self.N = int(universe_size)
        self.C = [[0 for i in range(self.k)] for j in range(self.t)]
        self.H = get_pw_ind_hash_funcs(self.t, self.N, self.k)

    def __enter__(self):
        return self

    # def __exit__(self, *err):
    #     pass

    #process input character j with integer amount c
    #for c > 0
    def process(self, j, c):
        if c > 0:
            for i in range(self.t):
                self.C[i][hash_func(*self.H[i],j)] += c

    # Given query character a, return min result of sketch
    # f^_a = min_{1<= i <= t} C[i][h_i(a)]}
    def query(self, a):
        v = parse(a)
        r = self.C[0][hash_func(*self.H[0],v)]
        for i in range(self.t):
            r = min(r, self.C[i][hash_func(*self.H[i],v)])
        return r


#inherit all but query from CountMinSketch.
class CountMinMedianSketch(CountMinSketch):
    '''
    Based on CountMinSketch, but using median instead of min. 
    NOT the same as count sketch median 
    (which also employs hashing functions g:[n]->{1,-1}).


    Turnstile sketch (allowing c >= 0 and c < 0).

    Algorithm:
    init:
    c[1..t][1..k] <- 0, k = 2/epsilon, t = ceil(log(1/delta))
    choose t independent 2-universal hash funcs h_1..h_t: [n] -> [k]

    process(j,c):
    for i = 1 to t do:
      C[i][h_i(j)] <- C[i][h_i(j)] + c

    output:
    on query a, report f^_a = median_{1<= i <= t} C[i][h_i(a)]}
    '''

    #process input character j with constant c
    #for c >= 0 or c < 0
    def process(self, j, c):
        for i in range(self.t):
            # h = self.H[i]
            self.C[i][hash_func(*self.H[i],j)] += c

    # Given query character a, return median result of sketch
    # f^_a = median_{1_{1<= i <= t} g_i(alpha)C[i][h_i(alpha)]}
    def query(self, a):
        v = parse(a)
        r = [None]*self.t
        for i in range(self.t):
            r[i] = self.C[i][hash_func(*self.H[i],v)]
        r.sort()
        median_index = floor(self.t/2)
        return r[median_index]
