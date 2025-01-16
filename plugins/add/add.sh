source ~/anaconda3/etc/profile.d/conda.sh
conda activate cap

cd $5
MarsVars.py $1.atmos_$2_Ls$3\_$4.nc -add rho zfull &

