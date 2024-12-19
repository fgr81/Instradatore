source ~/anaconda3/etc/profile.d/conda.sh
conda activate cap

cd $4
MarsVars.py $1.atmos_$6_Ls$2\_$3_$5.nc -zdiff ucomp vcomp
