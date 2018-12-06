import argparse
import sys
import time
import json

import boto3
from botocore.exceptions import ClientError

args_list = ['source_table', 'destination_table', 'source_profile', 'destination_profile',
                 'destination_table_staging_read_capacity', 'destination_table_staging_write_capacity',
                 'destination_table_ultimate_read_capacity', 'destination_table_ultimate_write_capacity',
                 'copy_mode']

def user_input():
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument('-cf', '--config_file', help='config file name', type=str)
    parent_parser_args = parent_parser.parse_known_args()
    object_check = vars(parent_parser_args[0])

    if object_check['config_file']:
        arguments = {}
        for arg in args_list:
            arguments[arg] = None
        # Set the default value for table capacity
        arguments['destination_table_staging_read_capacity'] = 5
        arguments['destination_table_staging_write_capacity'] = 1000
        arguments['destination_table_ultimate_read_capacity'] = 5
        arguments['destination_table_ultimate_write_capacity'] = 5

        json_file = json.load(open(object_check['config_file']))
        for key, value in json_file.items():
            arguments[key] = value

    else:
        child_parser = argparse.ArgumentParser(description='Command line tool for copying AWS Dynamodb table.', parents=[parent_parser])
        child_parser.add_argument('-s', '--source_table', help='source table name', type=str, required=True)
        child_parser.add_argument('-d', '--destination_table', help='destination table name', type=str, required=True)
        child_parser.add_argument('-sp', '--source_profile', help='source profile name', type=str)
        child_parser.add_argument('-dp', '--destination_profile', help='source profile name', type=str)
        child_parser.add_argument('-sr', '--destination_table_staging_read_capacity', help='staging read capacity', type=int, default=5)
        child_parser.add_argument('-sw', '--destination_table_staging_write_capacity', help='staging write capacity', type=int, default=1000)
        child_parser.add_argument('-ur', '--destination_table_ultimate_read_capacity', help='source table name', type=int, default=5)
        child_parser.add_argument('-uw', '--destination_table_ultimate_write_capacity', help='source table name', type=int, default=5)
        child_parser.add_argument('--deep-copy', dest='copy_mode', action='store_true')
        child_parser.add_argument('--shallow-copy', dest='copy_mode', action='store_false')
        child_parser.set_defaults(copy_mode=False)

        child_parser_args = child_parser.parse_args()
        arguments = vars(child_parser_args)

    return arguments


class DynamoDBTableCopier(object):

    def __init__(self):
        pass

    def create_resource(self, profile=None):
        """

        Parameters
        ----------
        profile

        Returns
        -------

        """
        session = boto3.session.Session(profile_name=profile)
        dynamodb = session.resource('dynamodb')
        return dynamodb

    def create_src_table(self, table_name, profile=None):
        """

        Parameters
        ----------
        table_name
        profile

        Returns
        -------

        """
        session = boto3.session.Session(profile_name=profile)
        dynamodb = session.resource('dynamodb')
        dynamodb_client = dynamodb.meta.client

        try:
            response = dynamodb_client.describe_table(TableName=table_name)
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'ResourceNotFoundException':
                print('The source table {} does not exist! Please create the table first and try again.'.format(
                    table_name))
                return False
            else:
                raise
        else:
            table = dynamodb.Table(table_name)
            return table

    def create_dst_table(self, table_name, profile=None):
        """

        Parameters
        ----------
        table_name
        profile

        Returns
        -------

        """
        session = boto3.session.Session(profile_name=profile)
        dynamodb = session.resource('dynamodb')
        dynamodb_client = dynamodb.meta.client

        try:
            dynamodb_client.describe_table(TableName=table_name)
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'ResourceNotFoundException':
                table = dynamodb.Table(table_name)
                return table
            else:
                raise
        else:
            print('The destination table {} already existed! Please delete it first and try again.'.format(table_name))
            sys.exit(1)

    def describe_table(self, table):
        """

        Parameters
        ----------
        table

        Returns
        -------

        """
        print('Table Name: {}'.format(table.name))
        print('Table Attributes: {}'.format(
            ','.join(map(lambda d: '{0}({1})'.format(
                d['AttributeName'],
                d['AttributeType']), table.attribute_definitions))
        ))
        print('Table Key Schema: {}'.format(
            ','.join(map(lambda d: '{0}({1})'.format(
                d['AttributeName'],
                d['KeyType']), table.key_schema))
        ))
        print('Table Capacity: {0}(Read), {1}(Write)'.format(
            table.provisioned_throughput['ReadCapacityUnits'],
            table.provisioned_throughput['WriteCapacityUnits']))

    def shallow_copy(self, src_table, dst_table, profile=None, read_capacity=5, write_capacity=5):
        """

        Parameters
        ----------
        src_table
        dst_table
        profile
        read_capacity
        write_capacity

        Returns
        -------

        """
        try:
            # Create the new table with same attributions' type and schema,
            # default capacity is 5/5
            dynamodb = self.create_resource(profile)
            dynamodb.create_table(
                AttributeDefinitions=src_table.attribute_definitions,
                TableName=dst_table.table_name,
                KeySchema=src_table.key_schema,
                ProvisionedThroughput={
                    'ReadCapacityUnits': read_capacity,
                    'WriteCapacityUnits': write_capacity
                })
            print('Creating the destination table {} ...'.format(dst_table.table_name))
            dst_table.wait_until_exists()
            print('Created an empty table {0} as the duplication'
                  ' of the table {1}.'.format(dst_table.table_name, src_table.table_name))
        except ClientError:
            print('Failed to create the destination table {}.'.format(dst_table.table_name))
            sys.exit(1)

    def deep_copy(self, src_table, dst_table, read_capacity=5, write_capacity=5):
        """

        Parameters
        ----------
        src_table
        dst_table

        Returns
        -------

        """
        start_key = None
        keep_scanning = True

        try:
            start_time = time.time()
            with dst_table.batch_writer() as batch:
                while keep_scanning:
                    response = src_table.scan(ExclusiveStartKey=start_key) \
                            if start_key \
                            else src_table.scan()
                    print('Scanned {0} items from the source table {1}'.format(
                        response['Count'], src_table.name))
                    items = response['Items']
                    for item in items:
                        batch.put_item(Item=item)
                    if 'LastEvaluatedKey' in response:
                        start_key = response['LastEvaluatedKey']
                    else:
                        keep_scanning = False
            # Update the table's capacity
            dst_table.update(
                ProvisionedThroughput={
                    'ReadCapacityUnits': read_capacity,
                    'WriteCapacityUnits': write_capacity,
                }
            )
            end_time = time.time()
            print('Successfully copy and migrate the data from table {0} to table {1}, '
                    'spent {2} seconds.'.format(
                        src_table.table_name, dst_table.table_name, int(end_time-start_time))
            )
            time.sleep(10)
        except Exception:
            print('Some unexpected errors occurred when copying the table,'
                    ' deleting the duplication table...')
            dst_table.delete()
            dst_table.wait_until_not_exists()
            sys.exit(1)


    def run(self, arguments):
        source_table = self.create_src_table(arguments['source_table'], arguments['source_profile'])
        destination_table = self.create_dst_table(arguments['destination_table'], arguments['destination_profile'])
        if arguments['copy_mode']:
            print('Start copying table and migrating the data...')
            print('Below is the short description of the source table:')
            self.describe_table(source_table)
            self.shallow_copy(source_table, destination_table,
                arguments['destination_profile'],
                arguments['destination_table_staging_read_capacity'],
                arguments['destination_table_staging_write_capacity'])
            self.deep_copy(source_table, destination_table,
                arguments['destination_table_ultimate_read_capacity'],
                arguments['destination_table_ultimate_write_capacity'])
            print('Below is the short description of the destination table:')
            self.describe_table(destination_table)
            print('Finished!')
        else:
            print('Start copying the table without migrating the data...')
            print('Below is the short description of the source table:')
            self.describe_table(source_table)
            self.shallow_copy(source_table, destination_table,
                arguments['destination_profile'],
                arguments['destination_table_ultimate_read_capacity'],
                arguments['destination_table_ultimate_write_capacity'])
            print('Below is the short description of the destination table:')
            self.describe_table(destination_table)
            print('Finished!')


if __name__ == '__main__':
    arguments = user_input()
    copier = DynamoDBTableCopier()
    copier.run(arguments)
