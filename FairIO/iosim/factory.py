'''
Created on Jun 20, 2014

@author: xleon
'''

from cdecimal import Decimal
from iosim.disk import Disk, File
from iosim.tenant import Tenant
from iosim.locator import Locator

MIN_DISK_BW = 50
MAX_DISK_BW = 100

config = None

def create_disks(n, rnd):
    disks = []
    for i in xrange(n):
        c = (Decimal(rnd.randint(MIN_DISK_BW*1000, MAX_DISK_BW*1000))/1000).quantize(Decimal('.0001'))
        disks.append(Disk(i, c))
    return disks

def create_tenants(m, locator, rnd):
    tenants = []
    
    for i in xrange(m):
        if i == 0:
            w = 500#10000
        elif i == 1:
            w = 500#10
        else:
            w = 500#(i+1)*10 # equal weights
        tenant = Tenant(i, w, locator)
        tenants.append(tenant)
    return tenants

def create_locator(disks, max_replication, n_recovery, fanout, rnd):
    print "Creating Locator Table (FDS)..."
    locator = Locator(disks, max_replication, n_recovery, fanout, rnd)
    locator.initialize()
    return locator

def create_files(tenant, num_f, init_replication, max_replication, block_size, locator, rnd):
    # 0->sizeZIP-weightZIP-reverse
    # 1->sizeZIP-weightZIP-sorted
    # 2->sizeZIP-weightequal
    # 3->sizeequal-weightZIP
    # 4->sizeequal-weightequal
    
    if config.file_creation_policy == 0: # zipf size, zipg weight, reversed
        distr = get_zip_distr(2.4, num_f, 200, rnd)
        distr.sort()
        
        size = get_zip_distr(2.4, num_f, 200, rnd)
        size.sort()
        size.reverse()
        max_val = max(size)
        min_val = min(size)
        max_f = 1024.0*1024.0
        min_f = 64.0
        for i in xrange(len(size)):
            size[i] = min_f + (max_f - min_f) * float(size[i] - min_val) / float(max_val - min_val)
        
    elif config.file_creation_policy == 1: # zipf size, zipf weight
        distr = get_zip_distr(2.4, num_f, 200, rnd)
        distr.sort()
        
        size = get_zip_distr(2.4, num_f, 200, rnd)
        size.sort()
        max_val = max(size)
        min_val = min(size)
        max_f = 1024.0*1024.0
        min_f = 64.0
        for i in xrange(len(size)):
            size[i] = min_f + (max_f - min_f) * float(size[i] - min_val) / float(max_val - min_val)
    elif config.file_creation_policy == 2: # zipf size, same weight
        distr = [100 for i in xrange(num_f)]
        
        size = get_zip_distr(2.4, num_f, 200, rnd)
        max_val = max(size)
        min_val = min(size)
        max_f = 1024*1024
        min_f = 64
        for i in xrange(len(size)):
            size[i] = min_f + (max_f - min_f) * float(size[i] - min_val) / float(max_val - min_val)
    elif config.file_creation_policy == 3: # Same size, zipf weights
        size = [config.file_size for i in xrange(num_f)]
        distr = get_zip_distr(2.4, num_f, 200, rnd)
    elif config.file_creation_policy == 4: # Same size, same weights
        size = [config.file_size for i in xrange(num_f)]
        distr = [100 for i in xrange(num_f)]
        
    files = []
    k = init_replication
    
    i = 0
    for d in distr:
        files.append(File(int(size[i]), k, int(d), max_replication, block_size, locator))
        i += 1
        
    for f in files:
        tenant.add_file(f)
        
def get_zip_distr(alpha, num, repetitions, rnd):
    distr_avg = None
    for i in xrange(repetitions):
        distr = list(rnd.zipf(alpha, num))
        distr.sort()
        if distr_avg == None:
            distr_avg = distr
        else:
            for i in xrange(num):
                distr_avg[i] += min(100, distr[i])
    return [val / repetitions for val in distr_avg]

if __name__ == '__main__':
    import numpy
    
    for i in xrange(1, 300):
        rnd = numpy.random.RandomState()   
        l = get_zip_distr(2.4, 10, 400, rnd)
        print "AVG", i, "\t", l