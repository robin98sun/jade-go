#!/usr/bin/env bash
target_dir=$1
master=$2
master_ip=$3
agent_base=$4
agent_count=$5
agent_ip_start=$6

master_template=./devops/examples/example-env_variables-master.txt
agent_template=./devops/examples/example-env_variables-agent-1.txt

tmpfile=./tmp_speed-env_`date +"%s"`.tmp

# master
cat $master_template|sed "s/cluster-1/${master}/g" > $tmpfile
cat $tmpfile|sed "s/192.168.57.11/${master_ip}/g" > ${tmpfile}.tmp && mv ${tmpfile}.tmp ${tmpfile}
mv $tmpfile $target_dir/env_variables-master.txt

# agent
i=1
while [[ $i -le $agent_count ]];do
  hostname=${agent_base}${i}
  ip=${agent_ip_start}
  if [[ $i > 1 ]];then
    pi=1
    newIp=""
    for p in `echo ${ip}|tr '.' ' '`;do
      if [[ $pi == 1 ]];then
        newIp=${p}
      elif [[ $pi < 4 && $pi > 1 ]];then
        newIp="${newIp}.${p}"
      else
        np=`expr $p + 1`
        newIp="${newIp}.${np}"
      fi
      pi=`expr $pi + 1`
    done
    ip=$newIp
  fi

  cat $agent_template|sed "s/raspberry1/$hostname/g" > $tmpfile
  cat $tmpfile|sed "s/192.168.57.13/$ip/g" > ${tmpfile}.tmp && mv ${tmpfile}.tmp $tmpfile

  cat $tmpfile|sed "s/cluster-1/$master/g" > ${tmpfile}.tmp && mv ${tmpfile}.tmp $tmpfile
  cat $tmpfile|sed "s/192.168.57.11/$master_ip/g" > ${tmpfile}.tmp && mv ${tmpfile}.tmp $tmpfile

  mv $tmpfile $target_dir/env_variables-agent-${i}.txt
  i=`expr $i + 1`
done
