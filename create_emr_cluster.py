import boto3


def lambda_handler(event, context):

    client = boto3.client('emr',  region_name="eu-west-1")

    instances = {
        'MasterInstanceType': 'm3.xlarge',
        'SlaveInstanceType': 'm3.xlarge',
        'InstanceCount': 2,
        'InstanceGroups': [],
        'Ec2KeyName': 'test',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-423c330a',
        'EmrManagedMasterSecurityGroup': 'sg-0b7be5f8131931e5a',
        'EmrManagedSlaveSecurityGroup':  'sg-0233851503bb4e2db'
    }

    configurations = [
        {
            'Classification': 'yarn-site',
            'Properties': {
                'yarn.resourcemanager.scheduler.class': 'org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler'
            },
            'Configurations': []
        },
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3"
                    }
                }
            ]
        }
    ]

    response = client.run_job_flow(
        Name='DSM Datamart',
        LogUri='s3://test-sanju/emr-logs',
        ReleaseLabel='emr-5.30.0',
        Instances=instances,
        Configurations=configurations,
        Steps=[],
        BootstrapActions=[],
        Applications=[
            {'Name': 'Spark'},
            {'Name': 'Zeppelin'},
            {'Name': 'Ganglia'}
        ],
        VisibleToAllUsers=True,
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        AutoScalingRole='EMR_AutoScaling_DefaultRole',
        EbsRootVolumeSize=30
    )
    return response["JobFlowId"]
