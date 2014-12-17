'''
Created on May 22, 2014

@author: xleon
'''
from iosim.optimizer import Optimizer

from numpy.random import RandomState

from iosim import factory

class Experiment(object):
    '''
    classdocs
    '''


    def __init__(self, env, config):
        '''
        Constructor
        '''
        self.config = config
        # Locator info
        n = self.config.num_disks
        n_recovery = self.config.num_disks_recovery
        fanout = self.config.fanout
        # Users info
        m = self.config.num_users
        # Files info
        f = self.config.num_files
        if self.config.max_replication > self.config.num_disks:
            max_replication = self.config.num_disks
        else:
            max_replication = self.config.max_replication
            
        init_replication = self.config.init_replication
        block_size = self.config.block_size
        factory.config = self.config
        self.rnd = RandomState(self.config.random_seed)
        
        disks = factory.create_disks(n, self.rnd)
        self.locator = factory.create_locator(disks, max_replication, n_recovery, fanout, self.rnd)
        tenants = factory.create_tenants(m, self.locator, self.rnd)
        self.locator.tenants = tenants
        for tenant in tenants:
            factory.create_files(tenant, f, init_replication, max_replication, block_size, self.locator, self.rnd)
            tenant.initialize_files()
        
        self.env = env
        self.env.process(self.run())
        
    def optimize(self):
        # Look for optimal allocation
        optim = Optimizer(self.locator, self.rnd, self.config)
        #cost_total, cost_total_baseline = optim.compute_allocation_byfile_withoutWA()
        optim.compute_allocation_byfile()
    
    def run(self):  
     
        self.optimize()

        yield self.env.timeout(100)
        