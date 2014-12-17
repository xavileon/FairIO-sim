'''
Created on May 22, 2014

@author: xleon
'''

from math import sqrt
import sys
from cdecimal import *
from bitarray import bitarray
import time
#from blist import sortedset

MIN_SHARE   = Decimal("0.0001")

class Tenant(object):
    '''
    classdocs
    '''


    def __init__(self, uid, weight, locator):
        self.uid = uid
        self.weight = Decimal(weight)
        self.locator = locator
        self.weights_bytenant = dict()
        self.weights_byfile = dict()
        self.k = 0
        self.ratio = 0
        self.files = []
    
    def __repr__(self):
        return "T"+str(self.uid)+"-"+str(self.weight)
    
    def initialize_files(self):
        for f in self.files:
            table_size = self.locator.get_table_size()
            init_idx = self.locator.get_index(f)
            for replica_num in xrange(f.replicas):
                for block_list in f.blocks:
                    block = block_list[replica_num]
                    row = (init_idx + block.bid) % table_size
                    disk = self.locator.get_disk(row, replica_num)
                    disk.add_block(block)
                    
    def initialize_weights(self):
        for f in self.files:
            f_share = self.get_file_weight(f)
            #interested = self.locator.get_disks_file(f)
            interested = f.get_interested_disks()
            alldisks = bitarray(len(self.locator.disks))
            alldisks.setall(True)
            not_interested = alldisks ^ interested 
            num_interested = interested.count()
            for disk in self.locator.bitarray_to_disks(interested):
                #initially, put the same weight (considering the global weoght) on all nodes in which the user is initerested
                disk.set_weight_byfile(self, f, f_share/num_interested)
                #disk.set_weight_byfile(self, f, f_share/num_interested)
                # initially, put the same weight on all nodes to get a fair share
                disk.set_weight_byfile(self, f, 10)
            for disk in self.locator.bitarray_to_disks(not_interested):
                disk.set_weight_byfile(self, f, 0)

    def set_namenode(self, namenode):
        self.locator = namenode
    
    def add_file(self, f):
        self.files.append(f)
        
    def remove_file(self, f):
        self.files.remove(f)
    
    def get_files(self):
        return self.files
    
    def get_tenant_weight(self):
        return self.weight / self.locator.get_total_weights()
    
    def get_file_weight(self, f):
        return self.get_tenant_weight() * (f.weight / self.get_total_weight_byfile())
    
    def get_total_weight_byfile(self):
        total_weight = 0
        for f_all in self.files:
            total_weight += f_all.weight
        return total_weight
    
    def get_ratio_bytenant(self):
        bw = self.get_utility_bytenant()
        target = self.locator.get_total_capacity() * self.get_tenant_weight()
        return bw / target, bw, target
    
    def get_ratio_byfile(self):
        ratios = dict()
        for f in self.files:
            bw_f = self.get_utility_byfile(f)
            target = self.locator.get_total_capacity() * self.get_file_weight(f)
            ratios[f] = (bw_f / target, bw_f, target)
        return ratios
    
    def get_utility_bytenant(self):
        #disks_bits = self.locator.get_disks_tenant(self)
        disks_bits = self.get_interested_disks()
        #disks = self.locator.bitarray_to_disks(disks_bits)
        sum_ut = 0
        for i in xrange(len(disks_bits)):
            if disks_bits[i]:
                disk = self.locator.disks[i]
                sum_ut += disk.capacity * disk.get_share_bytenant(self)
        return sum_ut
        #return sum([disk.capacity * disk.get_share_bytenant(self) for disk in disks])
    
    def get_utility_byfile(self, fid):
        #disks_bits = self.locator.get_disks_file(fid)
        disks_bits = fid.get_interested_disks()
        #disks = self.locator.bitarray_to_disks(disks_bits)
        sum_ut = 0
        for i in xrange(len(disks_bits)):
            if disks_bits[i]:
                disk = self.locator.disks[i]
                sum_ut += disk.capacity * disk.get_share_byfile(fid)
        return sum_ut
        #return sum([disk.capacity * disk.get_share_byfile(fid) for disk in disks])
    
    def get_interested_disks(self):
        interested = bitarray(len(self.locator.disks))
        interested.setall(False)
        for f in self.files:
            interested |= f.get_interested_disks()
        return interested
    
#             
#     def compute_optimal_shares_bytenant(self):
#         disks = self.locator.get_disks_tenant(self)
#         marginals = [(disk.capacity / disk.get_total_weight_bytenant(),
#                       Decimal("1.0"), # 1 because I'm interested in this node 
#                       disk.capacity, 
#                       disk.get_total_weight_bytenant(),
#                       disk.get_weight_bytenant(self),
#                       disk)
#                      for disk in disks]
#         budget = self.weight
#         self.compute_optimal_shares_common(budget, marginals, MIN_SHARE, "set_weight_bytenant", None)
    
    def compute_optimal_shares_byfile_total(self, fid):
        #disks_bits = self.locator.get_disks_file(fid)
        disks_bits = fid.get_interested_disks()
        #disks = self.locator.bitarray_to_disks(disks_bits)
        marginals = []
        for i in xrange(len(disks_bits)):
            if disks_bits[i]:
                disk = self.locator.disks[i]
                marginals.append((disk.capacity / disk.get_total_weights(),
                                  1,
                                  disk.capacity,
                                  disk.get_total_weights(),
                                  disk.get_weight_byfile(fid),
                                  disk
                                  )
                                 )
#         marginals = [(disk.capacity / disk.get_total_weights(), # marginal value
#                       Decimal("1.0"), # 1 because I'm interested in this node
#                       disk.capacity, # capacity for files
#                       disk.get_total_weights(), # yj
#                       disk.get_weight_byfile(fid), # xij
#                       disk) for disk in disks]
        
        budget = self.get_file_weight(fid)
        self.compute_optimal_shares_common_new(budget, marginals, MIN_SHARE, "set_weight_byfile", fid)
        
    def compute_optimal_shares_common(self, budget, marginals, min_share, update_function, fid):
        marginals.sort()
        marginals.reverse()
        #print marginals

        # Compute optimal largest k
        sub_total_sqrt_wy = 0
        sub_total_y = 0
        last_coeff = 0
        k = 0;
        for _, wij, cj, yj, xij, disk in marginals:
            sqrt_wy = (wij * yj * cj).sqrt()
            sub_total_sqrt_wy += sqrt_wy;
            sub_total_y += yj;
            coeff = (budget + sub_total_y) / sub_total_sqrt_wy;
            t = (sqrt_wy * coeff) - yj;
            
            tmin = t - ((min_share*yj)/(1-min_share))
            
            if tmin >= 0:
                k+=1
                last_coeff = coeff;
            else: 
                break
            
        # Compute optimal bid price based on last_coeff and k
        i = 0
        self.k = k 
        sum_xij = 0
        sum_cap = 0
        while i < k:
            _, wij, cj, yj, old_xij, disk = marginals[i]
            xij = ((wij*yj*cj).sqrt() * last_coeff) - yj
            #if self.uid == 0: 
            #    print self, xij, yj, disk.capacity
            sum_xij += xij
            sum_cap += disk.capacity * (xij/(yj-old_xij+xij))
            #self.shares_by_node[disk] = xij
            print disk, fid, fid.size, fid.weight, xij
            getattr(disk, update_function)(self, fid, xij) # Update weight on disk
            i+=1
        
        while i < len(marginals):
            _, wij, cj, yj, old_xij, disk = marginals[i]
            xij = 0
            #if self.uid == 0: print self, xij, yj, disk.capacity
            #sum_xij += xij
            print disk, fid, fid.size, fid.weight, xij
            #self.shares_by_node[disk] = xij
            getattr(disk, update_function)(self, fid, xij) # Update weight on disk
            i+=1

        #print self, sum_xij, sum_cap
        #if self.uid == 0:  self, self.get_weight(), sum_xij, sum_cap, self.get_utility_bytenant()

    def compute_optimal_shares_common_new(self, budget, marginals, min_share, update_function, fid):
        marginals.sort()
        marginals.reverse()
        #print marginals

        # Compute optimal largest k
        sub_total_sqrt_wy = 0
        sub_total_y = 0
        min_coeff = min_share/(1-min_share)
        sub_total_mins = sum([yj*min_coeff for _, _, _, yj, _, _ in marginals])
        last_coeff = 0
        k = 0;
        for _, wij, cj, yj, xij, disk in marginals:
            sqrt_wy = (yj * cj).sqrt()
            sub_total_sqrt_wy += sqrt_wy;
            sub_total_y += yj;
            sub_total_mins -= yj*min_coeff
            coeff = (budget - sub_total_mins + sub_total_y) / sub_total_sqrt_wy;
            t = (sqrt_wy * coeff) - yj;
            
            tmin = t - ((yj*min_share)/(1-min_share))
            
            if tmin >= 0:
                k+=1
                last_coeff = coeff;
            else: 
                break
            
        # Compute optimal bid price based on last_coeff and k
        i = 0
        self.k = k 
        #sum_xij = 0
        #sum_cap = 0
        while i < k:
            _, wij, cj, yj, old_xij, disk = marginals[i]
            xij = ((yj*cj).sqrt() * last_coeff) - yj
            #bids.append((xij/(xij+yj), xij))
            #sum_xij += xij
            #sum_cap += disk.capacity * (xij/(yj-old_xij+xij))
            getattr(disk, update_function)(self, fid, xij) # Update weight on disk
            i+=1
        
        while i < len(marginals):
            _, wij, cj, yj, old_xij, disk = marginals[i]
            xij = yj * min_coeff
            #bids.append((xij/(xij+yj), xij))
            getattr(disk, update_function)(self, fid, xij) # Update weight on disk
            i+=1
        #print max(bids), min(bids)
    
    def compute_extra_space(self):
        cost = 0
        for f in self.files:
            current_replicas = f.get_current_replicas()
            active_replicas = f.get_active_replicas()
            if active_replicas - current_replicas > 0:
                cost += f.numblocks * (active_replicas - current_replicas)
        return cost
    
    def compute_baseline_space(self):
        cost = 0
        for f in self.files:
            cost += f.numblocks * f.get_min_replication()
        return cost