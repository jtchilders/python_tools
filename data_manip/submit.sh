#!/bin/bash
#COBALT -n 8
#COBALT -t 60
#COBALT -q debug-cache-quad
#COBALT -A datascience

# app build with GNU not Intel
module swap PrgEnv-intel PrgEnv-gnu
# Use Cray's Application Binary Independent MPI build
module swap cray-mpich cray-mpich-abi

# include CRAY_LD_LIBRARY_PATH in to the system library path
export LD_LIBRARY_PATH=$CRAY_LD_LIBRARY_PATH:$LD_LIBRARY_PATH
# also need this additional library
export LD_LIBRARY_PATH=/opt/cray/wlm_detect/1.3.2-6.0.6.0_3.8__g388ccd5.ari/lib64/:$LD_LIBRARY_PATH
# in order to pass environment variables to a Singularity container create the variable
# with the SINGULARITYENV_ prefix
export SINGULARITYENV_LD_LIBRARY_PATH=$LD_LIBRARY_PATH


RANKS_PER_NODE=8
TOTAL_RANKS=$(( $COBALT_PARTSIZE * $RANKS_PER_NODE ))

aprun -n $TOTAL_RANKS -N $RANKS_PER_NODE singularity exec  -B /opt/cray/pe/mpt/7.7.3/gni/mpich-gnu-abi/5.1/lib/libmpi.so.12:/miniconda3/4.5.12/lib/libmpi.so.12:ro -B /opt/cray -B /projects/atlasMLbjets/parton /soft/datascience/singularity/conda_images/miniconda040512_py36_ml2.simg python /home/parton/git/python_tools/data_manip/numpy_to_sparse_numpy_calo2d.py -g "/projects/atlasMLbjets/parton/numpy_data/zee2jets/*.npz" -n 100 -o /projects/atlasMLbjets/parton/hd5_data/zee2jets_sparse_calo2d/output -z

