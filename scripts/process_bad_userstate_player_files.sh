#!/bin/sh
GAME=${1}
if [ "x${GAME}" = "x" ]; then
    echo "Pass india or dane as the first argument"
    exit 1
fi
for j in 0 1 2; do
    mkdir -p /var/log/${GAME}_userstates${j}/bad.archive
    for i in 0 1 2; do
        for f in $(ls /var/log/${GAME}_userstates${j}/user_state-modified-${i}-*.*.bad 2> /dev/null); do
	    echo ${f}
            PYTHONPATH="/var/www" python /var/www/uuss/scripts/process_userstates_buckets_infile.py -i /var/www/uuss/server/production.${GAME}.ini -b 100 -f ${f} -u ${i} -s ${j} -g ${GAME} || exit 1
            mv ${f} /var/log/${GAME}_userstates${j}/bad.archive
        done
    done
done
