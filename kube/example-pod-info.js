{
    TypeMeta:{
        Kind: 
        APIVersion:
    } 
    ObjectMeta:{
        Name:aces-diamonds-ace-robin98-jade-app-t-ta5ykw5fc7f366b736ac8p5wmm 
        GenerateName:aces-diamonds-ace-robin98-jade-app-t-ta5ykw5fc7f366b736ac8-64b8c78c68- 
        Namespace:default 
        SelfLink: 
        UID:3b3a40f1-b0a6-4dbd-a02e-94ddc04afecc 
        ResourceVersion:82838771 
        Generation:0 
        CreationTimestamp:2023-02-09 08:03:39 +0000 UTC 
        DeletionTimestamp:<nil> 
        DeletionGracePeriodSeconds:<nil> 
        Labels:map[jade-app:robin98-jade-app-temp-hum jade-app-module:aggregator jade-app-replica-index:replica-0 jade-app-version:3-autoscaling-1.3.2-2 jade-env:jadelet jade-node:aces-diamonds-ace jade-owner:aces.uta.edu jade-role:application pod-template-hash:64b8c78c68] Annotations:map[] 
        OwnerReferences:[{
            APIVersion:apps/v1 
            Kind:ReplicaSet 
            Name:aces-diamonds-ace-robin98-jade-app-t-ta5ykw5fc7f366b736ac8-64b8c78c68 
            UID:931198f9-130f-4bd3-963a-925da2f51feb 
            Controller:0xc00050fb8a 
            BlockOwnerDeletion:0xc00050fb8b}] 
        Finalizers:[] 
        ManagedFields:[{Manager:k3s Operation:Update APIVersion:v1 Time:2023-02-09 08:03:39 +0000 UTC FieldsType:FieldsV1 FieldsV1:{"f:metadata":{"f:generateName":{},"f:labels":{".":{},"f:jade-app":{},"f:jade-app-module":{},"f:jade-app-replica-index":{},"f:jade-app-version":{},"f:jade-env":{},"f:jade-node":{},"f:jade-owner":{},"f:jade-role":{},"f:pod-template-hash":{}},"f:ownerReferences":{".":{},"k:{\"uid\":\"931198f9-130f-4bd3-963a-925da2f51feb\"}":{}}},"f:spec":{"f:containers":{"k:{\"name\":\"robin98-jade-app-temp-hum\"}":{".":{},"f:env":{".":{},"k:{\"name\":\"JADE_APP_MODULE\"}":{".":{},"f:name":{},"f:value":{}},"k:{\"name\":\"JADE_APP_NAME\"}":{".":{},"f:name":{},"f:value":{}},"k:{\"name\":\"JADE_APP_VERSION\"}":{".":{},"f:name":{},"f:value":{}},"k:{\"name\":\"JADE_MASTERNODE_ADDR\"}":{".":{},"f:name":{},"f:value":{}},"k:{\"name\":\"JADE_MASTERNODE_PORT\"}":{".":{},"f:name":{},"f:value":{}},"k:{\"name\":\"JADE_MASTERNODE_PROTOCOL\"}":{".":{},"f:name":{},"f:value":{}},"k:{\"name\":\"JADE_PROVISIONING_TASK\"}":{".":{},"f:name":{},"f:value":{}},"k:{\"name\":\"accuracy\"}":{".":{},"f:name":{},"f:value":{}},"k:{\"name\":\"agent_id\"}":{".":{},"f:name":{},"f:value":{}},"k:{\"name\":\"avg_temperature\"}":{".":{},"f:name":{},"f:value":{}},"k:{\"name\":\"jade-addon-env-metrics\"}":{".":{},"f:name":{},"f:value":{}},"k:{\"name\":\"location\"}":{".":{},"f:name":{},"f:value":{}},"k:{\"name\":\"master_id\"}":{".":{},"f:name":{},"f:value":{}}},"f:image":{},"f:imagePullPolicy":{},"f:name":{},"f:ports":{".":{},"k:{\"containerPort\":8080,\"protocol\":\"TCP\"}":{".":{},"f:containerPort":{},"f:protocol":{}}},"f:resources":{".":{},"f:limits":{".":{},"f:cpu":{},"f:memory":{}},"f:requests":{".":{},"f:cpu":{},"f:memory":{}}},"f:terminationMessagePath":{},"f:terminationMessagePolicy":{}}},"f:dnsPolicy":{},"f:enableServiceLinks":{},"f:nodeSelector":{},"f:restartPolicy":{},"f:schedulerName":{},"f:securityContext":{},"f:terminationGracePeriodSeconds":{}}} Subresource:} {Manager:k3s Operation:Update APIVersion:v1 Time:2023-02-09 08:03:52 +0000 UTC FieldsType:FieldsV1 FieldsV1:{"f:status":{"f:conditions":{"k:{\"type\":\"ContainersReady\"}":{".":{},"f:lastProbeTime":{},"f:lastTransitionTime":{},"f:status":{},"f:type":{}},"k:{\"type\":\"Initialized\"}":{".":{},"f:lastProbeTime":{},"f:lastTransitionTime":{},"f:status":{},"f:type":{}},"k:{\"type\":\"Ready\"}":{".":{},"f:lastProbeTime":{},"f:lastTransitionTime":{},"f:status":{},"f:type":{}}},"f:containerStatuses":{},"f:hostIP":{},"f:phase":{},"f:podIP":{},"f:podIPs":{".":{},"k:{\"ip\":\"10.42.0.69\"}":{".":{},"f:ip":{}}},"f:startTime":{}}} Subresource:status}]
        } 
    Spec:{
        Volumes:[{
            Name:kube-api-access-zm46v 
            VolumeSource:{
                HostPath:nil 
                EmptyDir:nil 
                GCEPersistentDisk:nil 
                AWSElasticBlockStore:nil 
                GitRepo:nil 
                Secret:nil 
                NFS:nil 
                ISCSI:nil 
                Glusterfs:nil 
                PersistentVolumeClaim:nil 
                RBD:nil 
                FlexVolume:nil 
                Cinder:nil 
                CephFS:nil 
                Flocker:nil 
                DownwardAPI:nil 
                FC:nil 
                AzureFile:nil 
                ConfigMap:nil 
                VsphereVolume:nil 
                Quobyte:nil 
                AzureDisk:nil 
                PhotonPersistentDisk:nil 
                Projected:&ProjectedVolumeSource{
                    Sources:[]
                    VolumeProjection{
                        VolumeProjection{
                            Secret:nil,
                            DownwardAPI:nil,
                            ConfigMap:nil,
                            ServiceAccountToken:&ServiceAccountTokenProjection{
                                Audience:,ExpirationSeconds:*3607,Path:token,
                            },
                        },
                        VolumeProjection{
                            Secret:nil,
                            DownwardAPI:nil,
                            ConfigMap:&ConfigMapProjection{
                                LocalObjectReference:LocalObjectReference{
                                    Name:kube-root-ca.crt,
                                },
                                Items:[]KeyToPath{
                                    KeyToPath{
                                        Key:ca.crt,
                                        Path:ca.crt,
                                        Mode:nil,
                                    },
                                },
                                Optional:nil,
                            },
                            ServiceAccountToken:nil,
                        },
                        VolumeProjection{
                            Secret:nil,
                            DownwardAPI:&DownwardAPIProjection{
                                Items:[]DownwardAPIVolumeFile{
                                    DownwardAPIVolumeFile{
                                        Path:namespace,
                                        FieldRef:&ObjectFieldSelector{
                                            APIVersion:v1,
                                            FieldPath:metadata.namespace,
                                        },
                                        ResourceFieldRef:nil,
                                        Mode:nil,
                                    },
                                },
                            },
                            ConfigMap:nil,
                            ServiceAccountToken:nil,
                        },
                    },
                    DefaultMode:*420,
                } 
                PortworxVolume:nil 
                ScaleIO:nil 
                StorageOS:nil 
                CSI:nil 
                Ephemeral:nil
            }
        }] 
        InitContainers:[] 
        Containers:[{
            Name:robin98-jade-app-temp-hum 
            Image:robin98/jade-app-temp-hum:3-autoscaling-1.3.2-2--amd64 
            Command:[] 
            Args:[] 
            WorkingDir: 
            Ports:[{
                Name: 
                HostPort:0 
                ContainerPort:8080 
                Protocol:TCP 
                HostIP:
            }] 
            EnvFrom:[] 
            Env:[
                {Name:JADE_APP_NAME Value:robin98/jade-app-temp-hum ValueFrom:nil}   
                {Name:JADE_APP_VERSION Value:3-autoscaling-1.3.2-2 ValueFrom:nil} 
                {Name:JADE_APP_MODULE Value:aggregator ValueFrom:nil} 
                {Name:JADE_PROVISIONING_TASK Value:aces.uta.edu:robin98/jade-app-temp-hum:3-autoscaling-1.3.2-2:WVtuWUfda6e7b7f0d4d5d ValueFrom:nil} 
                {Name:JADE_MASTERNODE_ADDR Value:129.107.206.218 ValueFrom:nil} 
                {Name:JADE_MASTERNODE_PORT Value:31408 ValueFrom:nil} 
                {Name:JADE_MASTERNODE_PROTOCOL Value:http ValueFrom:nil} 
                {Name:location 
                Value:;value://LA 
                ValueFrom:nil} 
                {Name:accuracy 
                Value:;value://city 
                ValueFrom:nil} 
                {Name:avg_temperature 
                Value:get;http://176.0.0.3/temperature;timespan=int 
                ValueFrom:nil} 
                {Name:master_id 
                Value:;value://master1 
                ValueFrom:nil} 
                {Name:agent_id 
                Value:;value://agent36 
                ValueFrom:nil} 
                {Name:jade-addon-env-metrics 
                Value:get;http://129.107.206.131:8765/metrics 
                ValueFrom:nil}
            ]
            Resources:{Limits:map[cpu:{i:{value:1 scale:0} d:{Dec:<nil>} s:1 Format:DecimalSI} memory:{i:{value:1048576000 scale:0} d:{Dec:<nil>} s: Format:BinarySI}] Requests:map[cpu:{i:{value:1 scale:0} d:{Dec:<nil>} s:1 Format:DecimalSI} memory:{i:{value:1048576000 scale:0} d:{Dec:<nil>} s: Format:BinarySI}]} 
            VolumeMounts:[{Name:kube-api-access-zm46v ReadOnly:true MountPath:/var/run/secrets/kubernetes.io/serviceaccount SubPath: MountPropagation:<nil> SubPathExpr:}] 
            VolumeDevices:[] 
            LivenessProbe:nil 
            ReadinessProbe:nil 
            StartupProbe:nil 
            Lifecycle:nil 
            TerminationMessagePath:/dev/termination-log 
            TerminationMessagePolicy:File 
            ImagePullPolicy:IfNotPresent 
            SecurityContext:nil 
            Stdin:false StdinOnce:false TTY:false
        }] 
        EphemeralContainers:[] 
        RestartPolicy:Always 
        TerminationGracePeriodSeconds:0xc00050fd38 
        ActiveDeadlineSeconds:<nil> 
        DNSPolicy:ClusterFirst 
        NodeSelector:map[kubernetes.io/hostname:aces-diamonds-ace] 
        ServiceAccountName:default 
        DeprecatedServiceAccount:default 
        AutomountServiceAccountToken:<nil> 
        NodeName:aces-diamonds-ace 
        HostNetwork:false 
        HostPID:false 
        HostIPC:false 
        ShareProcessNamespace:<nil> 
        SecurityContext:&PodSecurityContext{
            SELinuxOptions:nil,RunAsUser:nil,RunAsNonRoot:nil,SupplementalGroups:[],FSGroup:nil,RunAsGroup:nil,Sysctls:[]Sysctl{},WindowsOptions:nil,FSGroupChangePolicy:nil,SeccompProfile:nil,
        } 
        ImagePullSecrets:[] 
        Hostname: 
        Subdomain: 
        Affinity:nil 
        SchedulerName:default-scheduler 
        Tolerations:[
            {Key:node.kubernetes.io/not-ready Operator:Exists Value: Effect:NoExecute TolerationSeconds:0xc00050fd70} 
            {Key:node.kubernetes.io/unreachable Operator:Exists Value: Effect:NoExecute TolerationSeconds:0xc00050fd90}
        ] 
        HostAliases:[] 
        PriorityClassName: 
        Priority:0xc00050fd9c 
        DNSConfig:nil 
        ReadinessGates:[] 
        RuntimeClassName:<nil> 
        EnableServiceLinks:0xc00050fda0 
        PreemptionPolicy:0xc000119b50 
        Overhead:map[] 
        TopologySpreadConstraints:[] 
        SetHostnameAsFQDN:<nil> 
        OS:nil 
        HostUsers:<nil>
    } 
    Status:{
        Phase:Running 
        Conditions:[{
            Type:Initialized 
            Status:True 
            LastProbeTime:0001-01-01 00:00:00 +0000 UTC 
            LastTransitionTime:2023-02-09 08:03:39 +0000 UTC 
            Reason: 
            Message:
        } {
            Type:Ready 
            Status:True 
            LastProbeTime:0001-01-01 00:00:00 +0000 UTC 
            LastTransitionTime:2023-02-09 08:03:52 +0000 UTC 
            Reason: 
            Message:
        } {Type:ContainersReady Status:True LastProbeTime:0001-01-01 00:00:00 +0000 UTC LastTransitionTime:2023-02-09 08:03:52 +0000 UTC Reason: Message:
        } {Type:PodScheduled Status:True LastProbeTime:0001-01-01 00:00:00 +0000 UTC LastTransitionTime:2023-02-09 08:03:39 +0000 UTC Reason: Message:
        }] 
        Message: 
        Reason: 
        NominatedNodeName: 
        HostIP:129.107.206.218 
        PodIP:10.42.0.69 
        PodIPs:[{IP:10.42.0.69}] 
        StartTime:2023-02-09 08:03:39 +0000 UTC 
        InitContainerStatuses:[] 
        ContainerStatuses:[{
            Name:robin98-jade-app-temp-hum 
            State:{
                Waiting:nil 
                Running:&ContainerStateRunning{
                    StartedAt:2023-02-09 08:03:51 +0000 UTC,
                } 
                Terminated:nil
            } 
            LastTerminationState:{
                Waiting:nil 
                Running:nil 
                Terminated:nil
            } 
            Ready:true RestartCount:0 
            Image:robin98/jade-app-temp-hum:3-autoscaling-1.3.2-2--amd64 
            ImageID:docker-pullable://robin98/jade-app-temp-hum@sha256:28ca5a9894eac17d9477d33da5eae6fcc081d1fe3179d353ec0e7a5b9e00728b 
            ContainerID:docker://bd79d3b19f6227780261768692e2d5b3126ef0377d20db1626a499641d02340d 
            Started:0xc00050fe0a
        }] 
        QOSClass:Guaranteed 
        EphemeralContainerStatuses:[]
    }
}