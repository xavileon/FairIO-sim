'''
Created on Jun 19, 2014

@author: xleon
'''
from iosim.disk import Disk
from cdecimal import Decimal
from numpy.random import RandomState
from bitarray import bitarray
import hashlib
import itertools
import sys
import copy

class Locator(object):
    '''
    classdocs
    '''


    def __init__(self, disks, max_replication, num_recovery, fanout, rnd):
        '''
        Constructor
        '''
        self.disks = disks
        self.num_disks = len(disks)
        self.max_replication = max_replication
        self.fanout = fanout
        self.num_recovery = num_recovery
        self.table = []
        self.rnd = rnd
        self.tenants = None
        self.total_capacity = sum([x.capacity for x in self.disks])
    
    def __str__(self):
        i = 0
        res  = "--------------------------------------------------------\n"
        res += "ROW".rjust(4)
        for k in xrange(self.max_replication):
            res += ("R" + str(k)).rjust(4)
        res += "\n--------------------------------------------------------\n"
        for row in self.table:
            res+= str(i).rjust(4)
            for cell in row:
                res += str(cell).rjust(4)
            i += 1
            res+="\n"
        return res
    
    def initialize(self):
        #for _ in xrange(self.num_disks*(self.num_disks-1)):
        if self.num_recovery >= self.num_disks:
            self.num_recovery = self.num_disks - 1
        for _ in xrange(self.num_disks*(self.num_recovery)):
            self.table.append([])

        # Add first two columns, all pairs
        i = 0
        for x in self.disks:
            aux = list(self.disks)
            aux.remove(x)
            self.rnd.shuffle(aux)
            for y in aux[:self.num_recovery]:
                self.table[i].append(x)
                self.table[i].append(y)
                i += 1

#         allpairs = itertools.permutations(self.disks, 2)
#         i = 0
#         for pairs in allpairs:
#             for pair in pairs:
#                 self.table[i].append(pair)
#             i += 1
        
        # Add the rest of columns up to maximum_replication -2
        for row in self.table:
            aux = list(self.disks)
            # Remove existing pair
            for cell in row:
                aux.remove(cell)    
            
            self.rnd.shuffle(aux)    
            for _ in xrange(self.max_replication-2):
                disk = aux.pop()
                row.append(disk)
                
        self.rnd.shuffle(self.table)
        # Add fanout permutations
        shallow_copy = copy.copy(self.table)
        for _ in xrange(1, self.fanout):
            aux_shallow_copy = copy.copy(shallow_copy)
            self.rnd.shuffle(aux_shallow_copy)
            self.table += aux_shallow_copy
        i = 0
        for row in self.table:
            assert len(row) == self.max_replication, "row %d -> %d != %d" %(i, len(row), self.max_replication)
            i += 1

    def count_disks(self):
        counts = dict()
        for row in self.table:
            for column in row:
                old_count = counts.setdefault(column, 0)
                counts[column] = old_count +1
                
        import pylab as P
        x = []
        for k,v in counts.items():
            print k, v
            x.append(v)
        P.figure()  
        P.hist(x)
        P.show()

    def get_num_disks(self):
        return len(self.disks)

    def get_total_capacity(self):
        return self.total_capacity            
    
    def get_total_weights(self):
        total_weights = 0
        for tenant in self.tenants:
            total_weights += tenant.weight
        return total_weights
    
    def get_disk(self, row, column):
        return self.table[row][column]
    
    def get_table_size(self):
        return len(self.table)
    
    def get_index(self, f):
        m = hashlib.sha1()
        m.update(str(f))
        return int(m.hexdigest(), 16) % len(self.table)
    
    # return value: bitarray with 1 in index of disk interested
    def get_disks_tenant(self, tenant):
        #interested = set()
        interested = bitarray(len(self.disks))
        interested.setall(False)
        for f in tenant.files:
            interested_f = self.get_disks_file(f)
            interested |= interested_f
            #interested = interested.union(interested_f)
        return interested
    
    # return value: bitarray with 1 in index of disk interested
    def get_disks_file(self, f):
        interested = bitarray(len(self.disks))
        interested.setall(False)
        #interested = set()
        init_idx = self.get_index(f)
        table_size = len(self.table)
        
        if f.numblocks > table_size:
            interested.setall(True) # All nodes appear on the table size
        else:
            for i in xrange(f.numblocks):
                if interested.all():
                    break
                row = (init_idx + i) % table_size
                for replica in xrange(f.selected_replicas.length()):
                    if f.selected_replicas[replica]:
                        interested[self.table[row][replica].index] = True
                #interested_row = set([self.table[row][replica] 
                #    for replica in xrange(f.selected_replicas.length()) 
                #        if f.selected_replicas[replica]])
                #interested_row = set(self.table[row][:f.active_replicas])
                #interested = interested.union(interested_row)
        return interested
    
    # return value: bitarray with 1 in index of disk interested
    def get_disks_file_replica(self, f, replica):
        interested = bitarray(len(self.disks))
        interested.setall(False)
        #interested = set()
        init_idx = self.get_index(f)
        table_size = len(self.table)
        
        for i in xrange(f.numblocks):
            row = (init_idx + i) % table_size
            interested[self.table[row][replica].index] = True
            #interested.add(self.table[row][replica])
        return interested
    
    def disks_to_bitarray(self, disks):
        bita = bitarray(len(self.disks))
        bita.setall(False)
        for disk in disks:
            bita[disk.index] = True
    def bitarray_to_disks(self, bita):
        return [self.disks[i] for i in xrange(len(bita)) if bita[i]]
            
    def add_tenant(self, tenant):
        self.tenants.append(tenant)
    
    def check_safety(self):
        i = 0
        for row in self.table:
            assert len(row) == len(set(row)), "ROW %d -> %s" %(i, row)
            i += 1
        
if __name__ == '__main__':
     
    num_disks = 50
    max_replication = 5
    fanout = 5
     
    rnd = RandomState(43)
    from iosim import factory
    disks = factory.create_disks(num_disks, rnd)
    locator = factory.create_locator(disks, max_replication, fanout, rnd)
    print locator
    print "TOTAL CAPACITY", locator.get_total_capacity()
    locator.check_safety()
         
    import numpy as np
    import pylab as P
    x = []
    for row in locator.table:
        for cell in row:
            x.append(cell.index)
    P.figure()  
    P.hist(x, len(disks))
    P.show()

            
    
    