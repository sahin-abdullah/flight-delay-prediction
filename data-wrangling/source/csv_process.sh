cd $1;

filename=$(for f in *.zip; do d="${f:59:4}"; echo "$d"; done | uniq);

for name in $filename; 
	do 
	mkdir -p $1/$name;
done

count=0;
total=$(ls On_Time* | wc -l);
echo "       [=========================Unzipping Data================================]"
pstr="[#######################################################################]"
for zipfile in On_Time_Reporting_Carrier*.zip; 
	do 
	filename=$(d="${zipfile:59:4}"; echo "$d");
	unzip -q -j -o "$zipfile" '*.csv' -d $1/$filename;
	count=$(( $count + 1 ));
  	pd=$(( $count * 73 / $total ));
  	printf "\r%3d.%1d%% %.${pd}s" $(( $count * 100 / $total )) $(( ($count * 1000 / $total) % 10 )) $pstr;
done

foldername=$(for f in *.zip; do d="${f:59:4}"; echo "$d"; done | uniq);
count=0;
total=$(for f in *.zip; do d="${f:59:4}"; echo "$d"; done | uniq | wc -l);
echo -e "\n       [=====================Combining CSV Files===============================]"
find . -name "*.zip" -type f -delete
for name in $foldername; 
	do 
	cd $1/$name;
	awk 'FNR==1 && NR!=1{next;}{print}' *.csv | \
	csvcut -c 5,6,9,10,11,12,14,21,23,30,31,32,37,38,39,40,41,42,43,48,49,50,51,52,53,55,57,58,59,60,61 > $name.csv;
	ls --hide=$name.csv | xargs rm;
	count=$(( $count + 1 ));
  	pd=$(( $count * 73 / $total ));
  	printf "\r%3d.%1d%% %.${pd}s" $(( $count * 100 / $total )) $(( ($count * 1000 / $total) % 10 )) $pstr;
	mv $name.csv ..;
	cd ..;
	rmdir $name;
done

echo "       Done!";