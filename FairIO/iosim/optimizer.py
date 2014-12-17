'''
Created on May 27, 2014

@author: xleon
'''
import sys

from cdecimal import Decimal
import time

# DISK SELECTION POLICIES
SEQUENTIAL_SELECTION = 0        # 0 = SEQUENTIAL SELECTION FROM LOCATOR
MARGINAL_SELECTION = 1      # 1 = MARGINAL BANDWIDTH (bandwidth per unit of weight) 

class Optimizer(object):
    '''
    classdocs
    '''


    def __init__(self, locator, rnd, config):
        '''
        Constructor
        '''
        #self.env = env
        self.locator = locator
        self.rnd = rnd
        self.replica_selection_policy = config.replica_selection_policy
        self.min_ratio_gap = Decimal(config.min_ratio_gap)
        self.min_utility_gap = Decimal(config.min_utility_gap)
        self.target_mmr = Decimal(config.target_mmr)
        self.max_iters_ratio = config.max_iters_ratio
        self.max_iters_utility = config.max_iters_utility
        
        self.config = config
        #self.action = env.process(self.run())
    
    def compute_global_utility_byfile_total(self):
        sum_utilities = 0
        utilities_byfile = dict()
        for tenant in self.locator.tenants:
            for f in tenant.files:
                ut = tenant.get_utility_byfile(f)
                sum_utilities += ut
                #print tenant, f, ut
                utilities_byfile[f] = ut
        return sum_utilities, utilities_byfile
    
    def compute_global_ratios_byfile_total(self):
        ratios = dict()
        for tenant in self.locator.tenants:
            ratios.update(tenant.get_ratio_byfile())
        return ratios

    def compute_global_ratios_byfile(self, tenant):
        return tenant.get_ratio_byfile()

    def compute_global_ratios_bytenant_total(self):
        ratios = dict()
        for tenant in self.locator.tenants:
            #print tenant, tenant.get_ratio_bytenant()
            ratios[tenant] = tenant.get_ratio_bytenant()
        return ratios

    def is_utility_converged_byfile_total(self, last_utilities):
        gu, utilities_byfile = self.compute_global_utility_byfile_total()
        converged = True
        for f, ut in utilities_byfile.items():
            utility_gap = ut - last_utilities[f]
            if abs(utility_gap) < self.min_utility_gap:
                converged = converged and True
            else:
                converged = False
        return converged, gu, utilities_byfile
    
    def is_ratio_converged(self, ratios):
#         converged = True
        ratios_ordered = []
        for t, (rt, current, target) in ratios.items():
            ratios_ordered.append((rt, t))
        ratios_ordered.sort()
        max_r = ratios_ordered[-1][0]
        min_r = ratios_ordered[0][0]
        if (min_r / max_r) >= self.target_mmr:
            return True
        else:
            return False

    def select_replica_sequential(self, f):
        try:
            idx_r = f.selected_replicas.index(False)
            f.selected_replicas[idx_r] = True
            return True
        except:
            return False       
    
    def select_replica_marginals(self, f):
        current_disks = f.get_interested_disks()
        marginals = []
        
        # First look at the existing replicas not selected
        # Then look for new replicas not selected and not existing
        for idx in xrange(f.replicas, self.locator.max_replication):
            if not f.selected_replicas[idx]:
                replica_disks = f.get_interested_disks_replica(idx)
                diff_disks = current_disks ^ replica_disks
                sum_marginals = 0
                for disk_idx in xrange(len(diff_disks)):
                    if diff_disks[disk_idx]:
                        disk = self.locator.disks[disk_idx]
                        sum_marginals += disk.capacity / disk.get_total_weights()
                if f.existing_replicas[idx]:
                    marginals.append((0, sum_marginals, idx))
                else:
                    marginals.append((1, sum_marginals, idx))
        marginals.sort(key = lambda x: (x[0], -x[1], x[2])) 
    
        if len(marginals) == 0:
            return False
        f.selected_replicas[marginals[0][2]] = True
        return True  
        
    def increase_active_replicas_byfile(self, ratios):
        files = []
        for f, (rt, current, target) in ratios.items():
            files.append((rt, f))
        files.sort()
        i = 0
            
        if self.replica_selection_policy == SEQUENTIAL_SELECTION:
            f = files[i][1]
            rt = files[i][0]
            while not self.select_replica_sequential(f) and i < len(files)-1 and rt < 1:
                i += 1
                f = files[i][1]
                rt = files[i][0]
        elif self.replica_selection_policy == MARGINAL_SELECTION:
            f = files[i][1]
            rt = files[i][0]
            while not self.select_replica_marginals(f) and i < len(files)-1 and rt < 1:
                i += 1
                f = files[i][1]
                rt = files[i][0]
        else:
            print "No replica selection policy selected"
            sys.exit()
        if i == len(files) or rt >= 1:
            return None, None, 0
        else:    
            
            return None, f, None
    
    def compute_shares_byfile_total(self):
        utility_converged = False
        j = 0
        gu, last_utilities = self.compute_global_utility_byfile_total()   
        while not utility_converged and j < self.max_iters_utility:
            for tenant in self.locator.tenants:
                for f in tenant.files:
                    tenant.compute_optimal_shares_byfile_total(f)
            utility_converged, last_gu, last_utilities = self.is_utility_converged_byfile_total(last_utilities)
            j += 1
    
    def compute_allocation_byfile(self):
        for tenant in self.locator.tenants:
            for f in tenant.files:
                f.reset_active_replicas()
                # put bids to zero to the not active_replicas
            tenant.initialize_weights()
            
        
        ratios = self.compute_global_ratios_byfile_total()
        print "FAIRNESS RATIOS AFTER Random Placement (Random)" 
        print "(min_r = %.4f, max_r = %.4f, mmr = %.4f, avg = %.4f) %s" % self.format_ratios(ratios)
        print
        init_t1 = time.clock()
        self.compute_shares_byfile_total()
        ratios = self.compute_global_ratios_byfile_total()
        print "FAIRNESS RATIOS AFTER 1st Global Weight Allocation (BPP + GWA)"
        print "(min_r = %.4f, max_r = %.4f, mmr = %.4f, avg = %.4f) %s" % self.format_ratios(ratios)
        print
        print "START Dynamic Replication (BPP + GWA + DR)"
        ratio_converged = self.is_ratio_converged(ratios)
        i = 1
        while not ratio_converged and i < self.max_iters_ratio:
            tenant, f, disks = self.increase_active_replicas_byfile(ratios)
            if f is None:
                break
            self.compute_shares_byfile_total()
            ratios = self.compute_global_ratios_byfile_total()
            ratio_converged = self.is_ratio_converged(ratios)
            rf = self.format_ratios(ratios)
            print "  DR - Iter ", i, "\tIncrease #replicas of ", f, "to", f.get_active_replicas(), "\t", 
            print "(min_r = '%.4f', max_r = '%.4f', mmr = '%.4f', avg = '%.4f')" % (rf[0], rf[1], rf[2], rf[3])
            i += 1
        print "RATIOS AFTER Dynamic Replication (BPP + GWA + DR)"
        print "(min_r = %.4f, max_r = %.4f, mmr = %.4f, avg = %.4f) %s" % self.format_ratios(ratios)
        print
        
        end_t = time.clock()
        self.exec_time = end_t - init_t1 
        
        self.print_results()
        # Update the number of current replicas
        for tenant in self.locator.tenants:
            for f in tenant.files:
                f.set_current_replicas(f.get_active_replicas())
    
    def print_results(self):
        print "###############################"
        print "#     RESULTS                 #"
        print "###############################"
        print "----------------------"
        print "TENANT FAIRNESS RATIOS (min, max, MMR) + ordered tenant ratios"
        print "----------------------"
        ratios = self.compute_global_ratios_bytenant_total()
        print "%.2f %.2f %.2f %.2f %s" % self.format_ratios(ratios)
        print
        print "----------------------"
        print "FILE FAIRNESS RATIOS (min, max, MMR) + ordered file ratios"
        print "----------------------"
        print
        ratios = self.compute_global_ratios_byfile_total()
        print "%.2f %.2f %.2f %.2f %s" % self.format_ratios(ratios)
        print
        print "-----------------------"
        print "FILE STATUS (tenantid fileid (min_replicas, current_replicas, active_replicas) size weight)"
        print "-----------------------"

        for tenant in self.locator.tenants:
            for f in tenant.files:
                min_r, current_r, active_r, size, weight = f.get_status()
                print tenant, 
                print f, 
                print "\t(" + str(min_r)+", "+str(current_r)+", "+str(active_r)+")",
                print "\t", size, weight
        
        cost_total = 0
        cost_total_baseline = 0
        for tenant in self.locator.tenants:
            cost_tenant = tenant.compute_extra_space()
            cost_baseline = tenant.compute_baseline_space()
            cost_total += cost_tenant
            cost_total_baseline += cost_baseline
        rf = self.format_ratios(ratios)    
        print "----------------------" 
        print "SUMMARY OF RESULTS"
        print "----------------------"
        print "\tNumFiles:\t", self.config.num_files 
        print "\tBlockSize:\t", self.config.block_size 
        print "\tNumDisks:\t", self.config.num_disks
        print "\tTargetMMR:\t", self.target_mmr
        print "\tActualMMR:\t%.3f" % (rf[2])
        print "\tCost (blk):\t", cost_total
        print "\tBaseline (blk):\t", cost_total_baseline 
        print "\tCost (%):\t", cost_total * 100 / float(cost_total_baseline)
        print "\tTime (s):\t", self.exec_time 
    
    def get_ratios_ordered(self, ratios):
        ratios_ordered = []
        for idr, rt in ratios.items():
            ratios_ordered.append((rt[0], idr))
        ratios_ordered.sort()
        return ratios_ordered
    
    def format_ratios(self, ratios):
        ratios_ordered = []
        sum_ratios = 0 
        for idr, rt in ratios.items():
            sum_ratios += rt[0]
            ratios_ordered.append((float(rt[0]), idr))
            
        ratios_ordered.sort()
        MMR = ratios_ordered[0][0] / ratios_ordered[-1][0]
        return ratios_ordered[0][0], ratios_ordered[-1][0], MMR, sum_ratios/len(ratios_ordered), ratios_ordered
        #for rt, id in ratios_ordered:
        #    print id, "\t", rt