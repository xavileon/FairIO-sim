'''
Created on May 22, 2014

@author: xleon
'''

from cdecimal import Decimal
from bitarray import bitarray

BLOCK_SIZE   = 16
MIN_BID      = Decimal("0.00000000000001")
MIN_REPLICAS = 3
global FILE_ID
FILE_ID = 0


class Block(object):
    ''' Blocks are identified by filedid (fid), blockid (bid) and replicaid (rid) '''
    def __init__(self, fid, bid, rid):
        self.fid = fid
        self.bid = bid
        self.rid = rid
        
    def __repr__(self):
        #fid_list = str(self.fd).split('-')[1:3]
        #fid_str = fid_list[0] + "-" + fid_list[1]
        #return "B-"+fid_str+"-"+str(self.bid)+"-"+str(self.rid)
        return "B-"+str(self.bid)+"-"+str(self.rid)+"-"+str(self.fid)
#         return "B-"+str(self.fid)

class File(object):
    def __init__(self, size, replicas, weight, max_replication, block_size, locator):
        global FILE_ID
        self.fid = FILE_ID
        FILE_ID += 1
#         self.fid = fid
        #self.numblocks = numblocks
        #self.size = numblocks * BLOCK_SIZE # We only allow file size multiple of block size
        self.numblocks = size / block_size
        self.block_size = block_size
        self.size = size
        self.replicas = replicas # replication factor minimum
        self.active_replicas = replicas
        self.current_replicas = replicas
        self.weight = Decimal(weight)
        self.blocks = []
        self.numdisks = locator.get_num_disks()
        self.selected_replicas = bitarray(max_replication)
        self.selected_replicas.setall(False)
        self.existing_replicas = bitarray(max_replication)
        self.existing_replicas.setall(False)
        # Cache interested disks by replica
        self.map_interested_disks = []
        for i in xrange(max_replication):
            bitcache = locator.get_disks_file_replica(self, i)
            self.map_interested_disks.append(bitcache)
            
        for i in xrange(self.replicas):
            self.selected_replicas[i] = True
            self.existing_replicas[i] = True
#         for i in xrange(self.numblocks):
#             self.blocks.append([])
#             for j in xrange(self.replicas):
#                 self.blocks[i].append(Block(self, i, j))
    
    def __repr__(self):
        #fid_str = str(self.fid).split('-')[1:3]
        #return "F-"+str(self.weight)+"-"+fid_str[0]+"-"+fid_str[1]
        return "F-"+str(self.fid)
    
    def reset_active_replicas(self):
        self.existing_replicas = bitarray(self.selected_replicas)
        self.selected_replicas.setall(False)
        #self.selected_replicas[0] = True
        for i in xrange(self.replicas):
            self.selected_replicas[i] = True
    
    # return bitarray
    def get_interested_disks(self):
        interested = bitarray(self.numdisks)
        interested.setall(False)
        for i in xrange(len(self.selected_replicas)):
            if self.selected_replicas[i]:
                interested |= self.map_interested_disks[i]
        return interested
    
    def get_interested_disks_replica(self, replica):
        return self.map_interested_disks[replica]
    
    def get_current_replicas(self):
        return self.current_replicas
    
    def get_active_replicas(self):
        return self.selected_replicas.count()
    
    def set_current_replicas(self, current_replicas):
        self.current_replicas = current_replicas
    
    def get_min_replication(self):
        return self.replicas

    def get_status(self):
        return (self.replicas, self.current_replicas, self.get_active_replicas(), self.size, self.weight)

class Disk(object):
    '''
    classdocs
    '''


    def __init__(self, index, capacity):
        self.index = index
        self.capacity = Decimal(capacity)
        #self.tenant_weight_byfiles = dict()
        #self.tenant_weight_bytenant = dict()
        self.weights = dict()
        self.total_weights = 0
        self.weights_tenant = dict()
        self.stored_blocks = []
        
    def __repr__(self):
        return str(self.index)
        #return str(self.id)+"-"+str(self.capacity)       
        
    def __str__(self):
        return str(self.index)
    
    def add_block(self, block):
        self.stored_blocks.append(block)
        
    def remove_block(self, block):
        self.stored_blocks.remove(block)
        
    def get_blocks(self):
        return self.stored_blocks
    
    def get_files_with_share(self):
        blocks = []
        for block in self.stored_blocks:
            f1 = block.fid
            share_f = self.get_share_byfile(f1)
            weight_f = self.get_weight_byfile(f1)
            blocks.append((f1, share_f, weight_f))
        return blocks
    
    def get_num_positive_bids(self):
        num_positive_bids = 0
        for w in self.weights.values():
            if w > 0:
                num_positive_bids += 1
        return num_positive_bids
            
    def set_weight_byfile(self, tenant, fid, weight):
        old_weight = self.weights.setdefault(fid, 0)
        diff_weight = weight - old_weight
        
        self.weights[fid] = weight
        self.total_weights += diff_weight
        self.weights_tenant.setdefault(tenant, 0)
        self.weights_tenant[tenant] += diff_weight       
    
    def get_weight_byfile(self, fid):
        if fid in self.weights:
            return self.weights[fid]
        return 0
    
    def get_weight_bytenant(self, tenant):
        if tenant in self.weights_tenant:
            return self.weights_tenant[tenant]
        return 0
    
    def get_total_weights(self):
        if self.total_weights == 0:
            return MIN_BID
        return self.total_weights
      
    def get_share_bytenant(self, tenant):
        return self.get_weight_bytenant(tenant) / self.get_total_weights()
    
    def get_share_byfile(self, fid):
        return self.get_weight_byfile(fid) / self.get_total_weights()
    
    def get_all_shares(self):
        all_shares = []
        for tenant, files in self.tenant_weight_byfiles.items():
            w_t = self.get_weight_bytenant(tenant)
            s_t = self.get_share_bytenant(tenant)
            for f, w_f in files.items():
                s_f = self.get_share_byfile(tenant, f)
                all_shares.append((tenant, f, w_t, s_t, w_f, s_f))
        return all_shares        
    
    # Method for events
    
    def get_object(self, tenant, block):
        # Wait some time according to share simulating transfer time
        # Check if block is in this disk
        yield 
        pass
    
    def put_object(self, tenant, block):
        # Wait some time according to share simulating transfer time
        # Check if block is in this disk
        yield
        pass
