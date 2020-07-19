# Import packages of interest
import json
import boto3

from configparser import ConfigParser

# Get configuration details
config = ConfigParser()
config.read('config.cfg')

# Set connections to AWS Services
iam      = boto3.client('iam')
redshift = boto3.client('redshift')
ec2      = boto3.resource('ec2')

# Create an IAM role if one with the same name doesn't exist
iam_roles       = iam.list_roles()['Roles']
iam_role_exists = any([True if iam_role['RoleName'] == config['iam']['role_name'] else False for iam_role in iam_roles])

if iam_role_exists:
    print('Role already exists')
    print('\nProperties\n-----------------------------')
    role_props = iam.get_role(RoleName=config['iam']['role_name'])['Role']
    for key, value in role_props.items():
        print('{}: {}'.format(key, value))
else:
    try:
        role_props = \
            iam.create_role(Path                     = config['iam']['path'],
                            RoleName                 = config['iam']['role_name'],
                            AssumeRolePolicyDocument = json.dumps(
                                                        {
                                                        'Statement': [
                                                            {'Action':'sts:AssumeRole',
                                                            'Effect':'Allow',
                                                            'Principal':{'Service': 'redshift.amazonaws.com'}}],
                                                        'Version':'2012-10-17'
                                                        }
                                                        ),
                            Description              = config['iam']['description']
            )
    except Exception as e:
        print(e)

role_arn = iam.get_role(RoleName=config['iam']['role_name'])['Role']['Arn']
print(role_arn)

# Attach s3 read only access to the above role
response = \
    iam.attach_role_policy(RoleName=config['iam']['role_name'],
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )['ResponseMetadata']['HTTPStatusCode']

print('Successfully attached s3 read access to role' if response==200 else 'Policy attach failed!')


# Create a redshift cluster if one with the same name doesn't exist
clusters = redshift.describe_clusters()
cluster_names = [cluster['ClusterIdentifier'] for cluster in clusters['Clusters']]
cluster_exists = True if config['redshift']['identifier'] in cluster_names else False

if cluster_exists:
    print('Cluster already exists!')
    cluster_props = redshift.describe_clusters(ClusterIdentifier=config['redshift']['identifier'])['Clusters'][0]
    print('Cluster exists.\nProperties\n-----------------------------')
    for key, value in cluster_props.items():
        print('{}: {}'.format(key, value))
else:
    try:
        cluster_props = \
            redshift.create_cluster(DBName             = config['redshift']['db_name'],
                                    ClusterIdentifier  = config['redshift']['identifier'],
                                    ClusterType        = config['redshift']['cluster_type'],
                                    NodeType           = config['redshift']['instance_type'],
                                    MasterUsername     = config['redshift']['db_user'],
                                    MasterUserPassword = config['redshift']['db_password'],
                                    NumberOfNodes      = int(config['redshift']['node_count']),
                                    Port               = int(config['redshift']['db_port']),
                                    IamRoles           = [role_arn]
            )
        print('\nProperties\n-----------------------------')
        for key, value in cluster_props.items():
            print('{}: {}'.format(key, value))
    except Exception as e:
        print(e)

# Open port 5439 on the redshift to enable communication
# Get the relavent security group for the VPC
sg_id  = redshift.describe_clusters(ClusterIdentifier=config['redshift']['identifier'])['Clusters'][0]['VpcSecurityGroups'][0]['VpcSecurityGroupId']
sg = ec2.SecurityGroup(sg_id)

try:
    sg.authorize_ingress(
            GroupName  = sg.group_name,
            CidrIp     = '0.0.0.0/0',
            IpProtocol = 'TCP',
            FromPort   = int(config['redshift']['db_port']),
            ToPort     = int(config['redshift']['db_port'])
        )
except Exception as e:
    print(e)


