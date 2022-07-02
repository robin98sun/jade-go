#!/usr/bin/env bash

function check_git_branch_exist() {
    git_branch=$1
    if [[ "$git_branch" == "" ]];then
        echo 0
    fi

    git branch |grep "$git_branch"|wc -l|awk '{print $1}'
}

check_git_branch_exist $1 

if [[ `check_git_branch_exist $1` -gt 0 ]];then
    echo haha
else
    echo nona
fi

