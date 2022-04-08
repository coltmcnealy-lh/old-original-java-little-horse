import yaml

postgres_deployment = {
    'apiVersion': 'apps/v1',
    'kind': 'Deployment',
    'metadata': {'creationTimestamp': None,
                 'labels': {'app': 'postgres'},
                 'name': 'postgres'},
    'spec': {
        'replicas': 1,
        'selector': {
            'matchLabels': {
                'app': 'postgres'}},
        'strategy': {
            'rollingUpdate': {
                'maxSurge': 1,
                'maxUnavailable': 1
            },
            'type': 'RollingUpdate'
        },
        'template': {
            'metadata': {
                'creationTimestamp': None,
                'labels': {'app': 'postgres'}},
            'spec': {
                'containers': [
                    {
                        'envFrom': [
                            {
                                'configMapRef': {'name': 'postgres'}
                            }],
                        'image': 'postgres:10.4',
                        'imagePullPolicy': 'IfNotPresent',
                        'name': 'postgres',
                        'ports': [
                            {
                                 'containerPort': 5432,
                                 'protocol': 'TCP'
                            }],
                        'resources': {},
                        'terminationMessagePath': '/dev/termination-log',
                        'terminationMessagePolicy': 'File',
                        'volumeMounts': [
                            {
                                'mountPath': '/var/lib/postgresql/data',
                                'name': 'postgredb'
                            }
                        ]
                    }
                ],
                'dnsPolicy': 'ClusterFirst',
                'restartPolicy': 'Always',
                'schedulerName': 'default-scheduler',
                'securityContext': {},
                'terminationGracePeriodSeconds': 30,
                'volumes': [
                    {'name': 'postgredb',
                             'persistentVolumeClaim': {
                                 'claimName': 'postgres'
                             }
                     }
                ]
            }
        }
    }
}

postgres_configmap = {
    'apiVersion': 'v1',
    'kind': 'ConfigMap',
    'metadata': {
        'name': 'postgres',
        'labels': {
            'app': 'postgres'}
    },
    'data': {
        'POSTGRES_DB': 'postgresdb',
        'POSTGRES_USER': 'postgresadmin',
        'POSTGRES_PASSWORD': 'admin123'
    }
}

postgres_service = {
    'apiVersion': 'v1',
    'kind': 'Service',
    'metadata': {
        'name': 'postgres',
        'labels': {
            'app': 'postgres'
        }
    },
    'spec': {
        'type': 'NodePort',
        'ports': [
            {
                'port': 5432
            }
        ],
        'selector': {
            'app': 'postgres'
        }
    }
}


print(yaml.dump(postgres_service))
