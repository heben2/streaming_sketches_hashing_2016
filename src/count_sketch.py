#!/bin/env python3

#import requests
from math import * #ceil, floor, log
from hashing import *
from stream import *
# from stream import parse_to_numeric

#wrapper parser function for input chars.
def parse(a):
    return parse_token(a)

class CountSketch:
    '''
    Turnstile sketch (allowing c >= 0 and c < 0).

    Algorithm:
    init:
    c[1..k] <- 0, k = 3/epsilon^2
    choose a random independent 2-universal hash funcs h: [n] -> [k]
    choose a random independent 2-universal hash funcs g: [n] -> {1,-1}

    process(j,c):
      C[h(j)] <- C[h(j)] + c*g(j)

    output:
    on query a, report f^_a = g(a)C[h(a)]}
    '''

    #N = source universe size
    #k = target universe size, based on accuracy parameter epsilon
    def __init__(self,universe_size,k):
        print("initializing count sketch.")
        self.k = int(k)
        self.N = int(universe_size) #to follow same naming as article for hash funcs
        self.C = [0]*self.k
        self.h = get_hash_function(self.N, self.k)
        self.g = get_hash_function(self.N, 2)

    def __enter__(self):
        return self

    # def __exit__(self, *err):
    #     pass

    #process input character j with integer amount c
    #for c >= 0 or c < 0
    def process(self, j, c):
        self.C[hash_func(*self.h,j)] += c * hash_func_binary(*self.g,j)

    # Given query character a, return result of sketch
    def query(self, a):
        v = parse(a)
        return hash_func_binary(*self.g,v)*self.C[hash_func(*self.h,v)]



class CountSketchMedian:
    '''
    Turnstile sketch (allowing c >= 0 and c < 0).

    Algorithm:
    init:
    c[1..t][1..k] <- 0, k = 3/epsilon^2, t = O(log(1/delta))
    choose t independent 2-universal hash funcs h_1..h_t: [n] -> [k]
    choose t independent 2-universal hash funcs g_1..g_t: [n] -> {1,-1}

    process(j,c):
    for i = 1 to t do:
      C[i][h_i(j)] <- C[i][h_i(j)] + c*g_i(j)

    output:
    on query a, report f^_a = median_{1<= i <= t} g_i(a)C[i][h_i(a)]}
    '''

    #N = source universe size
    #k = target universe size, based on accuracy parameter epsilon
    def __init__(self,universe_size,k,d=0.1,t=0):
        print("initializing count sketch median.")
        if d <= 0:
            #TODO throw exception
            d = 0.1

        self.t = int(t)        
        if t == 0:
            self.t = int(ceil(log(1/d,2)))
        self.k = int(k)

        self.N = int(universe_size) 
        self.C = [[0 for i in range(self.k)] for j in range(self.t)]
        self.H = get_pw_ind_hash_funcs(self.t, self.N, self.k)
        self.G = get_pw_ind_hash_funcs(self.t, self.N, 2)

    def __enter__(self):
        return self

    # def __exit__(self, *err):
    #     pass

    #process input character j with integer amount c
    #for c >= 0 or c < 0
    def process(self, j, c):
        for i in range(self.t):
            h = self.H[i]
            g = self.G[i]
            self.C[i][hash_func(*h,j)] += c * hash_func_binary(*g,j)

    # Given query character a, return median result of sketch
    # f^_a = median_{1_{1<= i <= t} g_i(alpha)C[i][h_i(alpha)]}
    def query(self, a):
        v = parse(a)
        r = [None]*self.t
        for i in range(self.t):
            h = self.H[i]
            g = self.G[i]
            r[i] = hash_func_binary(*g,v)*self.C[i][hash_func(*h,v)]
        r.sort()
        median_index = floor(self.t/2)
        return r[median_index]



