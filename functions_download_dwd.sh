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
		listurls $filename_grep $url | parallel -j 10 get_and_extract_one {}
		find ${filename} -empty -type f -delete # Remove empty files
		cdo -f nc copy -mergetime ${filename} ${1}_${year}${month}${day}${run}.nc
		rm ${filename}
		extract_members ${1}_${year}${month}${day}${run}.nc
		rm ${1}_${year}${month}${day}${run}.nc
	fi
}
export -f download_merge_2d_variable_icon_d2_eps
################################################
download_invariant_icon_d2_eps()
{
	# download grid
	filename="icon_grid_0047_R19B07_L.nc"
	wget -r -nH -np -nv -nd --reject "index.html*" --cut-dirs=3 -A "${filename}.bz2" "https://opendata.dwd.de/weather/lib/cdo/"
	bzip2 -d ${filename}.bz2
	# download hsurf
	filename="icon-d2-eps_germany_icosahedral_time-invariant_${year}${month}${day}${run}_000_0_hsurf.grib2"
	wget -r -nH -np -nv -nd --reject "index.html*" --cut-dirs=3 -A "${filename}.bz2" "https://opendata.dwd.de/weather/nwp/icon-d2-eps/grib/${run}/hsurf/"
	bzip2 -d ${filename}.bz2 
	cdo -f nc setgrid,icon_grid_0047_R19B07_L.nc:2 -copy ${filename} invariant_hsurf_${year}${month}${day}${run}.nc
	rm ${filename}
}
export -f download_invariant_icon_d2_eps
##############################################