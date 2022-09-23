listurls() {
	filename="$1"
	url="$2"
	wget -qO- $url | grep -Eoi '<a [^>]+>' | \
	grep -Eo 'href="[^\"]+"' | \
	grep -Eo $filename | \
	xargs -I {} echo "$url"{}
}
export -f listurls
#################
get_and_extract_one() {
  url="$1"
  file=`basename $url | sed 's/\.bz2//g'`
  if [ ! -f "$file" ]; then
  	wget -t 2 -q -O - "$url" | bzip2 -dc > "$file"
  fi
}
export -f get_and_extract_one
#################
extract_members() {
	filename="$1"
	# infer name of variable by looking inside netcdf
	name=`cdo vardes ${filename} | awk -F" " 'FNR == 1 {print $2}'`
	# ensemble members
	vals=($(seq 2 1 20))
	for i in "${vals[@]}"; do
		cdo chname,"${name}_${i}","${name}" -select,name="${name}_${i}" ${filename} ens${i}_${filename}
	done
	cdo chname,"${name}","${name}" -select,name="${name}" ${filename} ens1_${filename}
}
export -f extract_members
#################
merge_members() {
	vals=($(seq 1 1 20))
	for i in "${vals[@]}"; do
		cdo setgrid,icon_grid_0047_R19B07_L.nc:2 -merge ens${i}_*.nc merged_ens${i}.nc
		rm ens${i}_*.nc
	done
}
export -f merge_members
##############################################
download_merge_2d_variable_icon_d2_eps()
{
	filename="icon-d2-eps_germany_icosahedral_single-level_${year}${month}${day}${run}_*_${1}.grib2"
	filename_grep="icon-d2-eps_germany_icosahedral_single-level_${year}${month}${day}${run}_(.*)_${1}.grib2.bz2"
	url="https://opendata.dwd.de/weather/nwp/icon-d2-eps/grib/${run}/${1}/"
	if [ ! -f "${1}_${year}${month}${day}${run}.nc" ]; then
		listurls $filename_grep $url | parallel -j 5 get_and_extract_one {}
		find ${filename} -empty -type f -delete # Remove empty files
        # For total precipitation first order by time step so that CDO is able to handle it properly
		# and remove the data every 15 minutes as it is too much
		if [ "$1" == "tot_prec" ]; then
			for f in $filename; do
				grib_copy -B stepRange $f $f.2
				rm $f
				mv $f.2 $f
			done
			cdo seltime,00:00,01:00,02:00,03:00,04:00,05:00,06:00,07:00,08:00,09:00,10:00,11:00,12:00,13:00,14:00,15:00,16:00,17:00,18:00,19:00,20:00,21:00,22:00,23:00 -mergetime ${filename} ${1}_${year}${month}${day}${run}.grib2
		else
			cdo mergetime ${filename} ${1}_${year}${month}${day}${run}.grib2
		fi
		rm ${filename}
	fi
}
export -f download_merge_2d_variable_icon_d2_eps
##############################################
download_merge_3d_variable_icon_d2_eps()
{
	filename="icon-d2-eps_germany_icosahedral_pressure-level_${year}${month}${day}${run}_*_${1}.grib2"
	filename_grep="icon-d2-eps_germany_icosahedral_pressure-level_${year}${month}${day}${run}_(.*)_850_${1}.grib2.bz2"
	url="https://opendata.dwd.de/weather/nwp/icon-d2-eps/grib/${run}/${1}/"
	if [ ! -f "${1}_${year}${month}${day}${run}.nc" ]; then
		listurls $filename_grep $url | parallel -j 5 get_and_extract_one {}
		find ${filename} -empty -type f -delete # Remove empty files
		cdo mergetime ${filename} ${1}_${year}${month}${day}${run}.grib2
		rm ${filename}
	fi
}
export -f download_merge_3d_variable_icon_d2_eps
################################################
download_invariant_icon_d2_eps()
{
	# download grid
	filename="icon_grid_0047_R19B07_L.nc"
	wget -r -nH -np -nv -nd --reject "index.html*" --cut-dirs=3 -A "${filename}.bz2" "https://opendata.dwd.de/weather/lib/cdo/"
	bzip2 -d ${filename}.bz2
	# download hsurf
	# filename="icon-d2-eps_germany_icosahedral_time-invariant_${year}${month}${day}${run}_000_0_hsurf.grib2"
	# wget -r -nH -np -nv -nd --reject "index.html*" --cut-dirs=3 -A "${filename}.bz2" "https://opendata.dwd.de/weather/nwp/icon-d2-eps/grib/${run}/hsurf/"
	# bzip2 -d ${filename}.bz2 
	#cdo -f nc setgrid,icon_grid_0047_R19B07_L.nc:2 -copy ${filename} invariant_hsurf_${year}${month}${day}${run}.nc
	#rm ${filename}
}
export -f download_invariant_icon_d2_eps
##############################################