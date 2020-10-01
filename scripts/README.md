# scripts for the GNS beavan cluster

Some notes on using the opensha tools on this environement.

 - The beavan cluster is available inside GNS networks only.
 - It has 28 nodes with 16*2 Xeon cores and 256GB RAM, PLUS 1 node with 1TB and 32 * 2 cores.
 - jobs must be scheduled from the master node using the **Torque/PBS** tools notably `qsub, qstat & qdel`
 
 ## Prequisites 
 
 GNS do not extend the main OS, some pre-requisites must be installed by us. The `/opt/sw` folder is shared
 across all beavan nodes making this available for pbs jobs.
 
 required:
  
  - the Java8 runtime environment is installed to to `/opt/sw/java/java-se-8u11-ri`
  
## PBS scripts
  
 - the [/scripts](https://github.com/GNS-Science/nshm-nz-opensha/scripts) folder in this repo contains pbs scripts
     - build.pbs
     - build_fatjar.pbs (bulds everything, this is usually what you want)
     - crustal_inversion.pbs (CFM data > rutpures - > inversion)
     - crustal_rupts_nk.pbs  (CFM data > rutptures easier to mess with parameters
     - demo3.pbs
     - hikudemo2.pbs
