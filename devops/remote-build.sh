if [[ "$3" != "" ]];then 
    remote_host=$3
fi
if [[ "$4" != "" ]];then
    remote_account=$4
fi
if [[ "$5" != "" ]];then
    cmd=$5
fi
if [[ "$6" != "" ]];then
    registry=$6
fi
if [[ "$7" != "" ]];then
    tag=$7
fi
if [[ "$8" != "" ]];then
    registry_password=$8
fi

proxy_host=$1
proxy_account=$2

if [[ "$remote_host" == "" ]];then
    echo "please specify the remote host"
    exit 1
fi

if [[ "$remote_account" == "" ]];then
    echo "please specify the account on the remote host"
    exit 1
fi


# for working at home via VPN
if [[ "$proxy_account" != "" && "$proxy_host" != "" && "$proxy_account" != "none" && "$proxy_host" != "none" ]];then
    tmpfile=$0.tmp.sh
    echo "# begin of auto generated code" >$tmpfile
    echo "remote_host=$remote_host" >> $tmpfile
    echo "remote_account=$remote_account" >> $tmpfile
    echo "hostname" >>$tmpfile

    if [[ "$cmd" != "build" && "$cmd" != "push" && "$cmd" != "build-and-push" && "$cmd" != "" ]];then
        if [[ "$cmd" != "reuse" ]];then
            ssh ${proxy_account}@${proxy_host} <<!
                rm -rf ~/tmp/jadelet
                rm -f ~/jadelet.source.tar.gz
!
            scp $cmd ${proxy_account}@${proxy_host}:~/jadelet.source.tar.gz
            ssh ${proxy_account}@${proxy_host} <<!
                rm -rf ~/tmp/jadelet
                mkdir -p ~/tmp/jadelet
                cp ~/jadelet.source.tar.gz ~/tmp/jadelet
                cd ~/tmp/jadelet
                tar xzf jadelet.source.tar.gz
                if [[ \`find . -name "._*" -print|wc -l|awk '{print \$1}'\` != 0 ]];then
                    find . -name "._*" -print|while read line; do
                        echo \$line
                        rm -f \$line
                    done
                    rm -f jadelet.source.tar.gz
                    tar czf jadelet.source.tar.gz ./*
                    mv jadelet.source.tar.gz ~
                fi
                rm -rf ~/tmp/jadelet
!
        fi
        echo 'source_pack="~/jadelet.source.tar.gz"' >> $tmpfile
        echo "scp ~/jadelet.source.tar.gz ${remote_account}@${remote_host}:~/jadelet.source.tar.gz" >> $tmpfile
        # other commands
        echo "cmd=build-and-push" >> $tmpfile
    else
        echo "source_pack=''" >> $tmpfile
        echo "cmd='$cmd'" >> $tmpfile
    fi
    echo "registry='$registry'" >> $tmpfile
    echo "tag='$tag'" >> $tmpfile
    echo "registry_password='$registry_password'" >> $tmpfile
    echo "# end of auto generated code" >> $tmpfile

    cat $0 >> $tmpfile
    ssh ${proxy_account}@${proxy_host} 'bash -s' < $tmpfile
    rm -f $tmpfile
    exit
elif [[ "$cmd" != "build" && "$cmd" != "push" && "$cmd" != "build-and-push" ]];then
    if [[ "$cmd" != "reuse" ]];then
        ssh ${remote_account}@${remote_host} <<!
            rm -rf ~/tmp/jadelet
            rm -f ~/jadelet.source.tar.gz
!
        scp $cmd ${remote_account}@${remote_host}:~/jadelet.source.tar.gz
    fi
    ssh ${remote_account}@${remote_host} <<!
        rm -rf ~/tmp/jadelet
        mkdir -p ~/tmp/jadelet
        cp ~/jadelet.source.tar.gz ~/tmp/jadelet
        cd ~/tmp/jadelet
        tar xzf jadelet.source.tar.gz
        if [[ \`find . -name "._*" -print|wc -l|awk '{print \$1}'\` != 0 ]];then
            find . -name "._*" -print|while read line; do
                echo \$line
                rm -f \$line
            done
            rm -f jadelet.source.tar.gz
            tar czf jadelet.source.tar.gz ./*
            mv jadelet.source.tar.gz ~
        fi
        rm -rf ~/tmp/jadelet
!
    if [[ "$cmd" != "" ]];then
        cmd="build-and-push"
    fi
    source_pack='~/jadelet.source.tar.gz'
fi

id
hostname
workspace="~/Dev/src/jadelet"

ssh ${remote_account}@${remote_host} <<!
hostname
if [[ "\$GOROOT" == "" ]];then
    echo "no go root is defined"
    exit
fi

if [[ "\$GOPATH" == "" ]];then
    echo "no go path is defined"
    exit
fi

echo "workspace =" $workspace
# clear and re-establish the workplace
if [[ "$source_pack" != "" ]];then
    ls -l $source_pack

    if [[ \`ls -l $source_pack|grep jadelet|wc -l|awk '{print \$1}'\` == 0 ]];then
        echo "no jadelet source package"
        exit
    fi

    rm -rf $workspace
    mkdir -p $workspace
    mv $source_pack $workspace
    cd $workspace
    tar xzf jadelet.source.tar.gz
    rm -f jadelet.source.tar.gz
    cd -
fi 

ls -l $workspace

# build the application of jadelet  
if [[ "$cmd" == "build" || "$cmd" == "build-and-push" || "$cmd" == "" ]];then
    cd $workspace/jadesdk
    go install
    if [[ \$? != 0 ]];then exit; fi

    cd $workspace/jade-go
    go install
    if [[ \$? != 0 ]];then exit; fi

    go build -o app
    if [[ \$? != 0 ]];then exit; fi

    cd $workspace/plankton
    go install
    if [[ \$? != 0 ]];then exit; fi

    go build -o plankton
    if [[ \$? != 0 ]];then exit; fi
fi

# push to docker registry
if [[ "$tag" != ""  && "$registry" != "" ]];then
if [[ "$cmd" == "push" || "$cmd" == "build-and-push" ]];then

    if [[ "$registry_password" != "" ]];then
        sudo docker login --username $registry --password $registry_password
    fi
    cd $workspace/jade-go
    sudo docker image build --tag ${registry}/jadelet:${tag} .
    if [[ \$? != 0 ]];then exit; fi
    sudo docker push ${registry}/jadelet:${tag}
    if [[ \$? != 0 ]];then exit; fi

    cd $workspace/plankton
    sudo docker image build --tag ${registry}/plankton:${tag} .
    if [[ \$? != 0 ]];then exit; fi
    sudo docker push ${registry}/plankton:${tag}
    if [[ \$? != 0 ]];then exit; fi
fi
fi

echo "done"
exit
!
