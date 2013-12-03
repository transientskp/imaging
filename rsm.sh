set -x

echo `date`
echo $HOSTNAME
echo $HOME
echo $VO_LOFAR_SW_DIR

echo ""
echo "WN Architecture"
cat /proc/meminfo | grep "MemTotal"
cat /proc/cpuinfo | grep "model name"

echo ""
echo $PWD
ls -l $PWD

echo ""
echo "setting up the environment for the lofar commands"

echo ""
echo "setting LOFARROOT"
LOFARROOT=${VO_LOFAR_SW_DIR}/current/lofar/release
export LOFARROOT

echo ""
echo "sourcing lofarinit.sh from SW_DIR"
. ${VO_LOFAR_SW_DIR}/current/lofar/release/lofarinit.sh || exit 1

echo ""
echo "correcting PATH and LD_LIBRARY_PATH for incomplete settings in lofarinit.sh: "
PATH=$VO_LOFAR_SW_DIR/current/local/bin:$VO_LOFAR_SW_DIR/current/lofar/release/sbin:$VO_LOFAR_SW_DIR/current/lofar/release/bin:$PATH;
export PATH
LD_LIBRARY_PATH=$VO_LOFAR_SW_DIR/current/lofar/release/lib:$VO_LOFAR_SW_DIR/current/lofar/release/lib64:$VO_LOFAR_SW_DIR/current/local/lib:$VO_LOFAR_SW_DIR/current/local/lib64:$LD_LIBRARY_PATH;
export LD_LIBRARY_PATH
PYTHONPATH=$VO_LOFAR_SW_DIR/current/lofar/release/lib/python2.7/site-packages:$VO_LOFAR_SW_DIR/current/local/lib/python2.7/site-packages:$PYTHONPATH;
export PYTHONPATH

# NB we can't assume the home dir is shared across all Grid nodes.
echo ""
echo "adding SYMBOLIC LINK FOR EPHEMERIDES AND GEODETIC DATA into homedir"
ln -s $VO_LOFAR_SW_DIR/current/data ~/

echo ""
echo "START PROCESSING"

echo ""
echo "Downloading files from grid storage"
prep_files_dir=`mktemp -d`
mkdir ${prep_files_dir}/L107845
mkdir ${prep_files_dir}/L107846

sed -i "s%srm://srm.grid.sara.nl:8443%gsiftp://gridftp.grid.sara.nl:2811%g" L107845.txt
sed -i "s%$% file://${prep_files_dir}/L107845/%" L107845.txt

sed -i "s%srm://srm.grid.sara.nl:8443%gsiftp://gridftp.grid.sara.nl:2811%g" L107846.txt
sed -i "s%$% file://${prep_files_dir}/L107846/%" L107846.txt

# download files from LTA to worker node
globus-url-copy -f L107845.txt
globus-url-copy -f L107846.txt

echo ""
echo "Untarring files to input directory"
input_dir=`mktemp -d`
mkdir -p ${input_dir}/L107845
mkdir -p ${input_dir}/L107846

# Untar files
for i in ${prep_files_dir}/L107845/*.tar; do tar -xf $i -C L107845/L107845; done
for i in ${prep_files_dir}/L107846/*.tar; do tar -xf $i -C L107846/L107846; done

rm -rf ${prep_files_dir}
#
#echo ""
#echo "Untar scripts"
#tar -xvf scripts.tar
#echo ""
#echo "Untar skymodels"
#tar -xvf skymodels.tar
#
#echo ""
#echo "cleaning temp files in scratch from previous runs"
#rm -rf $TMPDIR/rsm
#echo "creating new temp file in TMPDIR"
#mkdir $TMPDIR/rsm
#
#echo ""
#echo "Executing imaging-multibeam.py"
#time python scripts/imaging-multibeam.py scripts/imaging-multibeam.parset > nohup.out
#
##echo ""
##echo " Tar output images"
#tar cvf output.tar output/
#
#echo ""
#echo "Copy output.tar and nohup.out to the worker node"
#srmrm -r srm://srm.grid.sara.nl:8443/pnfs/grid.sara.nl/data/lofar/user/disk/RSM/L107845/
#srmmkdir srm://srm.grid.sara.nl:8443/pnfs/grid.sara.nl/data/lofar/user/disk/RSM/L107845
#lcg-cp --vo lofar file:`pwd`/nohup.out srm://srm.grid.sara.nl:8443/pnfs/grid.sara.nl/data/lofar/user/disk/RSM/L107845/nohup.out
#lcg-cp --vo lofar file:`pwd`/output.tar srm://srm.grid.sara.nl:8443/pnfs/grid.sara.nl/data/lofar/user/disk/RSM/L107845/output.tar
#
#echo ""
#echo "List the files copied to the SE lofar/user/disk:"
#srmls srm://srm.grid.sara.nl:8443/pnfs/grid.sara.nl/data/lofar/user/disk/RSM/L107845
#
#echo ""
#echo "listing final files"
#ls -allh $PWD
#echo ""
#du -hs $PWD
#du -hs $PWD/*
#du -h *
#
#echo ""
#echo "cleaning temp files in scratch"
#rm -rf $TMPDIR/rsm

echo ""
echo `date`
echo "*** RSM finished successfully***"
