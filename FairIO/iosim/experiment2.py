'''
Created on May 22, 2014

@author: xleon
'''
from iosim.optimizer import Optimizer
from iosim.tenant import Tenant
from iosim.disk import File

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
        n = 10
        n_recovery = self.config.num_disks_recovery
        fanout = self.config.fanout
        # Users info
        m = 1
        # Files info
        f = 2
        if self.config.max_replication > n:
            max_replication = n
        else:
            max_replication = self.config.max_replication
            
        init_replication = self.config.init_replication
        init_replication = 1
        block_size = self.config.block_size
        block_size = 32
        factory.config = self.config
        self.rnd = RandomState(43)
        
        disks = factory.create_disks(n, self.rnd)
        self.locator = factory.create_locator(disks, max_replication, n_recovery, fanout, self.rnd)
        # single tenant
        tenants = [Tenant(0, 100, self.locator)]
        self.tenant = tenants[0]
        self.locator.tenants = tenants
        for tenant in tenants: # 1000 and 300
            self.bigfile = File(1024*1024, init_replication, 100, max_replication, block_size, self.locator)
            self.smallfile = File(32, init_replication, 1000, max_replication, block_size, self.locator)
            tenant.add_file(self.bigfile)
            tenant.add_file(self.smallfile)
            tenant.initialize_files()
        
        self.env = env
        self.env.process(self.run())
    
    def plot_bars(self, tag):
        values = []
        total_capacity = 0.0
        for disk in self.locator.disks:
            values.append((float(disk.capacity)*float(disk.get_share_byfile(self.smallfile)), float(disk.capacity)*float(disk.get_share_byfile(self.bigfile)), disk.index, disk.capacity))
            total_capacity += float(disk.capacity)
        
        values.sort(key=lambda x: x[3])
        values.reverse()
        values_small = []
        values_big = []
        values_x = []
        weights_x = []
        i = 0
        for fs, fb, d, c in values:
            values_small.append(fs)
            values_big.append(fb)
            values_x.append(i)
            i += 1
            weights_x.append(100*float(c)/total_capacity)
            
        import matplotlib.pyplot as plt
        fig = plt.figure()
        p1 = plt.bar(values_x, values_small, color='#FF8000', hatch="//",                   label="Small file - High weight".expandtabs())
        p2 = plt.bar(values_x, values_big, bottom=values_small, color='#660000', hatch="x", label="Big file - Small weight".expandtabs())
        plt.legend()
        x1,x2,y1,y2 = plt.axis()
        plt.axis((x1,x2,y1,150))
        plt.ylabel('Node capacity (Mbps)')
        plt.xlabel('Node')
        #plt.title('Scores by group and gender')
        #plt.xticks(ind+width/2., ('G1', 'G2', 'G3', 'G4', 'G5') )
        #plt.yticks(np.arange(0,81,10))
        #plt.legend( (p1[0], p2[0]), ('Men', 'Women') )
        #plt.savefig("test-"+tag+".pdf")
        plt.show()
    
    def run(self):  
        optim = Optimizer(self.locator, self.rnd, self.config)
        
        for tenant in self.locator.tenants:
            for f in tenant.files:
                f.reset_active_replicas()
                # put bids to zero to the not active_replicas
            tenant.initialize_weights()
        
        tag = "reversed"
        
        self.plot_bars(tag+"-RANDOM")
        
        optim.compute_shares_byfile_total()
        
        self.plot_bars(tag+"-WEIGHTED")
        
        optim.compute_allocation_byfile()
        
        self.plot_bars(tag+"-FINAL")

        yield self.env.timeout(100)
        