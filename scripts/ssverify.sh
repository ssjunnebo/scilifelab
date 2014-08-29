#!/bin/bash
#
# Verify file at Swestore
#
# Pontus Freyhult, Uppmax, 2012

if [ "$#" != 2 ]; then
  echo "Usage: $0 localpath irodspath"
  echo
  echo "$0 verifies that the localfile has been correctly uploaded to"
  echo "Swestore given it's path in irods."
  exit 1
fi

localpath="$1"
irods="$2"

if [ -r "$localpath" ]; then
  :
else
  echo "$localpath doesn't exist or is not readable for the current user,"
  echo "please make sure it can be read properly."
  exit 1
fi

if [ -d "$localpath" ]; then
  irods="${irods}/"
fi

sep=':'

count=0

find "$localpath" -type f |  while read filename; do


    irodsfilepath="`echo $filename | sed -e "s$sep$localpath$sep$irods$sep"`"

    adler=`adler32 "$filename"`
    size=`/usr/bin/stat -c%s "$filename"`

    test "$size" -eq 0 || echo "$adler:$size:$irodsfilepath" | ssh -o StrictHostKeyChecking=no -i ssverify.key irods@kali.uppmax.uu.se 2>/dev/null

    good=$?

    echo -n "Checking if $filename matches $irodsfilepath..."
    [ "$good" -ne 0 ] && exit 1

    echo " seems good!"

done

if [ $? -ne 0 ]; then
	echo " NO! Something is wrong! "
        echo "Either the files don't match or something went wrong, please contact "
        echo "support@uppmax.uu.se to investigate."
        exit 1
fi

# Everything seems to check out
echo "It seems $localpath matches $irods on Swestore."

exit 0
