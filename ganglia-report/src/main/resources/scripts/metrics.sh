#!/bin/bash

basedir="/tmp/ganglia"
dir=`ls /export/home/ganglia/rrds/bigdata\ cluster/bigdata101 | grep flume.CHANNEL`
for file in $dir
do
  host="bigdata101"
  channel=`echo $file | awk '{split($0, a, "."); print a[3]}'`
  type=`echo $file | awk '{split($0, a, "."); print a[4]}'`
  result=$host.$channel.$type
  rrdtool fetch /export/home/ganglia/rrds/bigdata\ cluster/bigdata101/$file AVERAGE --start `date -d '1 days ago 6' +%s` --end `date +%s` | grep `date -d '1 days ago 7' +%s` > $basedir/$result
  for((i=8;i<24;i++))
  do
    time=`date -d "1 days ago $i" +%s`
    rrdtool fetch /export/home/ganglia/rrds/bigdata\ cluster/bigdata101/$file AVERAGE --start `date -d '1 days ago 7' +%s` --end `date +%s` | grep $time >> $basedir/$result
  done
  for((i=0;i<8;i++))
  do
    time=`date -d "today $i" +%s`
    rrdtool fetch /export/home/ganglia/rrds/bigdata\ cluster/bigdata101/$file AVERAGE --start `date -d '1 days ago' +%s` --end `date +%s` | grep $time >> $basedir/$result
  done
done