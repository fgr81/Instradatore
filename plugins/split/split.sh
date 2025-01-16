source ~/anaconda3/etc/profile.d/conda.sh
conda activate cap

cd $5
MarsFiles.py $1.atmos_$2.nc -split $3 $4
