'''
Created on May 22, 2014

@author: xleon
'''

import simpy
from optparse import OptionParser

from cdecimal import getcontext, FloatOperation, ROUND_HALF_UP
import iosim
import importlib

# Initialize cdecimal module
c = getcontext()
c.prec = 12
c.traps[FloatOperation] = True
c.rounding = ROUND_HALF_UP

if __name__ == '__main__':    
    # Parse command line
    parser = OptionParser()
    parser.set_default("num_users", 1)
    parser.set_default("num_files", 50)
    parser.set_default("file_size", 64)
    parser.set_default("block_size", 16)
    parser.set_default("replica_selection_policy", 1)
    parser.set_default("file_creation_policy", 0)
    parser.set_default("fanout", 3)
    parser.set_default("num_disks", 200)
    parser.set_default("num_disks_recovery", 50)
    parser.set_default("max_replication", 100)
    parser.set_default("init_replication", 3)
    parser.set_default("random_seed", None)
    parser.set_default("min_ratio_gap", "0.05")
    parser.set_default("min_utility_gap", "0.01")
    parser.set_default("target_mmr", "0.95")
    parser.set_default("max_iters_ratio", 500)
    parser.set_default("max_iters_utility", 100)
    parser.set_default("experiment", "iosim.experiment.Experiment")

    parser.add_option("--experiment", dest="experiment", action="store", type="string",
                      help="Experiment class to instantiate (must be a simpy process)")
    parser.add_option("--target_mmr", dest="target_mmr", action="store", type="string",
                      help="Target Max-min ratio (0,1)")
    parser.add_option("--min_ratio_gap", dest="min_ratio_gap", action="store", type="string",
                      help="Minimum ratio difference between current MMR and 1 (convergence criteria)")
    parser.add_option("--min_utility_gap", dest="min_utility_gap", action="store", type="string",
                      help="Minimum utility gap between iterations (convergence criteria)")
    parser.add_option("--max_iters_ratio", dest="max_iters_ratio", action="store", type="int",
                      help="Maximum numbers of iterations before stoping (force stop criteria if not converged)")
    parser.add_option("--max_iters_utility", dest="max_iters_utility", action="store", type="int",
                      help="Maximum numbers of iterations before stoping (force stop criteria if not converged)")
    parser.add_option("--num_users", dest="num_users", action="store", type="int",
                      help="Number of tenants")
    parser.add_option("--num_files", dest="num_files", action="store", type="int",
                      help="Number of files per tenant")
    parser.add_option("--block_size", dest="block_size", action="store", type="int",
                      help="Block size in MB")
    parser.add_option("--fanout", dest="fanout", action="store", type="int",
                      help="Number of permutations of the initial location table to be concatenated")
    parser.add_option("--num_disks", dest="num_disks", action="store", type="int",
                      help="Number of disks")
    parser.add_option("--num_disks_recovery", dest="num_disks_recovery", action="store", type="int",
                      help="Number of disks to be used in recovery")
    parser.add_option("--max_replication", dest="max_replication", action="store", type="int",
                      help="Maximum replication degree for files")
    parser.add_option("--init_replication", dest="init_replication", action="store", type="int",
                      help="Initial replication degree for files (mandatory number of replicas)")
    parser.add_option("--random_seed", dest="random_seed", action="store", type="int",
                      help="Random seed if repeatable experiments are needed")
    parser.add_option("--replica_selection_policy", dest="replica_selection_policy", action="store", type="int",
                      help="Replica selection policy (0->SEQUENTIAL, 1->MARGINALS")
    parser.add_option("--file_creation_policy", dest="file_creation_policy", action="store", type="int",
                      help="Mapping between file size and weight (0->reverseCorrelation, 1->directCorrelation, 2->ZipSize-EqualWeight, 3->EqualSize-ZipWeight, 4->EqualAll")
    parser.add_option("--file_size", dest="file_size", action="store", type="int",
                      help="Size of file in case file_creation_policy == 3 or 4")
    parser.set_default("new_percentage", "100")
    parser.add_option("--new_percentage", dest="new_percentage", action="store", type="string",
                      help="Weight of the new user to be added")
    (config, args) = parser.parse_args()
    
    print "--------------------"
    print "CONFIGURATION VALUES"
    print "--------------------"
    for k,v in vars(config).items():
        print str(v).ljust(6), k
    print "--------------------"
    
    env = simpy.Environment()
                  
    
    #from iosim.experiment import Experiment
    mod_name, func_name = config.experiment.rsplit('.',1)
    mod = importlib.import_module(mod_name)
    func = getattr(mod, func_name)
    experiment = func(env, config)
    
    env.run(until=200)
    
    
    