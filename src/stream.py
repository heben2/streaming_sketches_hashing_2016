# from random import random.randint
from math import ceil, log
import json
from numpy import random


def output_list_to_file(path, description, o_list):
    f = open(path, 'a')
    f.write(description+'\n')
    json.dump(o_list,f)
    f.write('\n')
    # f.write(','.join(str(e) for e in o_list) + '\n')
    f.close()

# def output_dlist_to_file(path, description, o_dlist):
#     f = open(path, 'a')
#     f.write(description+'\n')
#     json.dump(o_dlist,f)
#     f.write('\n')
#     # ','.join(str(e) for e in o_dlist)
#     # f.write(','.join(str(e) for e in o_dlist) + '\n')
#     f.close()

def output_val_to_file(path, description, val):
    f = open(path, 'a')
    f.write(description+' = '+str(val)+'\n')
    f.close()

def trunc_file(path):
    f = open(path, 'w')
    f.close()


# def parse_to_numeric(a):
#     return int(a)

def parse_token(a):
    return int(a)

def parse_item(tuple_a):
    t,c = tuple_a
    return parse_token(t), int(c)


# from StringIO import StringIO
# >>> io = StringIO()
# >>> json.dump(['streaming API'], io)
# >>> io.getvalue()

#TODO delete
#Now use generic lenght of tokens and tokens are only numbers (multi digit)
#This does not apply now.
# def parse_to_numeric(a):
    # v = ord(a.lower()) - parser_offset_number
    # return v if v < 9 else v - parser_offset_char
#not sure if needed, reverse of parse_to_numeric()
# def parse_from_numeric(v):
#     return chr(v + parser_offset_number) if v < 9 else chr(v + parser_offset_total)


def read_tokens(path):
    fh = open(path, 'r')
    return fh.readline()

def get_token_list(path):
    return read_tokens(path).split(',')

def get_token_set(path):
    return set(get_token_list(path))

def get_token_size(path):
    return len(get_token_set(path))

def construct_uniform_dump(dump_path, token_path, dump_size, *args):
    token_set = get_token_list(token_path)
    n = len(token_set)
    items = [(token_set[random.randint(0,n-1)],1) for i in range(dump_size)]
    f = open(dump_path, 'w')
    # f.write('\n'.join(tokens))
    json.dump(items,f)
    f.close()

#p is the percentage of tokens using up half the dump_size compared to the whole
#stream.
#p assumed decimal, e.g. p=0.1 for 10%
#All roundings go up, to the resulting dump size might exceed given dump_size.
# def construct_unevenly_dist_dump(dump_path, token_path, dump_size, p):
#     token_set = get_token_list(token_path)
#     n = len(token_set)
    
#     s_count_sample_size = ceil(dump_size/4)
#     l_count_sample_size = ceil(s_count_sample_size*p)
#     # c = s_count_sample_size % l_count_sample_size
#     l_count_size = ceil(dump_size/l_count_sample_size)

#     items = [(token_set[random.randint(0,n-1)],1) for i in 
#               range(s_count_sample_size)]
#     items += [(token_set[random.randint(0,n-1)],l_count_size) 
#              for i in range(l_count_sample_size)]
#     # items += [(token_set[random.randint(0,n-1)],l_count_size+c)]

#     # items = [(token_set[i], random.randint(dump_size/2, dump_size) if random() <= p 
#     #            else random.randint(1,dump_size/4)) for i in range(n)]

#     f = open(dump_path, 'w')
#     json.dump(items,f)
#     # f.write('\n'.join(tokens))
#     f.close()



def construct_std_norm_dist_dump(dump_path, token_path, dump_size, *args):
    token_set = get_token_list(token_path)
    n = len(token_set)

    s = random.standard_normal(dump_size)
    items = [(token_set[random.randint(0,n-1)], ceil(pow(2,abs(i)*10) )) 
            for i in s]

    f = open(dump_path, 'w')
    json.dump(items,f)
    # f.write('\n'.join(tokens))
    f.close()



def construct_std_norm_dist_dump_m(dump_path, token_path, dump_size, *args):
    token_set = get_token_list(token_path)
    n = len(token_set)
    dump_p = log(dump_size, 2)
    # s_list = random.standard_normal(dump_size)
    shape = 0.
    scale = 4.
    s_list = random.normal(shape, scale, dump_size)
    p = max(abs(s_list))
    items = []
    m = 0
    i = 0
    while m < dump_size:
        # v = ceil(pow(2,abs(s_list[i])*dump_p/p))
        v = ceil(pow(2,abs(s_list[i])))
        if v+m > dump_size:
            v = dump_size - m
        items += [(token_set[random.randint(0,n-1)], v)]
        m += v
        i += 1

    f = open(dump_path, 'w')
    json.dump(items,f)
    f.close()
    # return items, m

# def construct_std_norm_dist_dump_m(dump_path, token_path, dump_size, *args):
#     token_set = get_token_list(token_path)
#     n = len(token_set)
#     dump_p = log(dump_size, 2)
#     s_list = random.standard_normal(dump_size)
#     items = []
#     m = 0
#     i = 0
#     while m < dump_size:
#         v = ceil(pow(10,abs(s_list[i])))
#         if v+m > dump_size:
#             v = dump_size - m
#         items += [(token_set[random.randint(0,n-1)], v)]
#         m += v
#         i += 1

#     f = open(dump_path, 'w')
#     json.dump(items,f)
#     f.close()
#     # return items, m



#path = where to dump tokens
#size = how many tokens to generate
#tokens are simply numbers from 0 to (size-1)
#in file, tokens are separated by ','
def construct_tokens(path, size):
    tokens = [str(i) for i in range(size)]
    f = open(path, 'w')
    f.write(','.join(tokens))
    f.close()
    return set(tokens)

#move cursor one line and return token read. Assumes a token is one line.
class Stream:
    m = 0 #length of processed stream
    i = 0
    l = 0
    # token_set = {}
    # universe_size = -1
    def __init__(self, path):
        try:
            # self.filehandler = open(path, 'r')
            f = open(path, 'r')
            self.items = json.load(f)
            self.l = len(self.items)
            f.close()
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
        except:
            print("file not found!")

    def __enter__(self):
        return self

    def __exit__(self, *err):
        del self.items
    #     # self.filehandler.close()

    def next(self):
        if self.i < self.l:
            item = self.items[self.i]
            self.i += 1
            self.m += item[1]
            return item[0], item[1]
        else:
            return '', -1

    def reset(self):
        self.i = 0
        self.m = 0

    #returns next 1-byte-char and moves cursor 1 byte
    # def next(self):
    #     #if parsing of special chars needed, do call to parser here.
    #     self.m += 1
    #     return self.filehandler.readline()
    #     #return self.filehandler.read(1)

    # def set_token_list(self, path):
    #     token_set = get_token_set(path)
    #     universe_size = len(token_set)