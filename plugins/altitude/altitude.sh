source ~/anaconda3/etc/profile.d/conda.sh
conda activate cap

cd $5
MarsInterp.py $1.atmos_$2_Ls$3\_$4.nc -t $6
