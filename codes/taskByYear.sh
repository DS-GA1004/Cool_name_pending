

module load python/gnu/3.4.4
module load spark/2.2.0
export PYSPARK_PYTHON='/share/apps/python/3.4.4/bin/python'
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.4.4/bin/python'


YEAR=$1
SPARKCODE=$(echo "$3".py)
PAIRNAME=$2
TMPFILE="${PAIRNAME}${YEAR}"

echo /usr/bin/hadoop fs -rm -r "${PAIRNAME}${YEAR}"

echo spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "${YEAR}"
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "${YEAR}"
echo /usr/bin/hadoop fs -getmerge "$TMPFILE" "$TMPFILE".tmp
/usr/bin/hadoop fs -getmerge "$TMPFILE" "$TMPFILE".tmp
echo cat "$TMPFILE".tmp > "$TMPFILE"
cat "$TMPFILE".tmp > "$TMPFILE"
echo rm "$TMPFILE".tmp
rm "$TMPFILE".tmp
