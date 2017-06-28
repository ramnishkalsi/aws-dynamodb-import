import boto3
import mysql.connector
import ConfigParser
import simplejson as json

import sys
reload(sys)
sys.setdefaultencoding('utf8')

config = ConfigParser.RawConfigParser()
config.read('ConfigFile.properties')

region_name = config.get("AWS", "region_name");
aws_access_key_id = config.get("AWS", "aws_access_key_id");
aws_secret_access_key=config.get("AWS", "aws_secret_access_key")

dynamodb = boto3.resource('dynamodb', region_name=region_name, aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
dynamodb_client = boto3.client('dynamodb', region_name=region_name, aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)


def import_row_for_item(table_name, csv_file_name, colunm_names, column_types):
    print ' importing a row'


def list_existing_tables():
    response = dynamodb_client.list_tables()
    return response["TableNames"]


# Create schema in dynamodb
def create_schema(table_name, key_schema, attributes):
    pt = {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 1}
    try:
        table = dynamodb_client.create_table(TableName=table_name, KeySchema=key_schema,
                                             AttributeDefinitions=attributes,
                                             ProvisionedThroughput=pt)

        # table creation is an async operation, so wait while it is created.
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        print ("table created", table)
        return table
    except:  # catch all exceptions
        print "Unexpected error:", sys.exc_info()


def main():
    print "--start--"

    tables_csv_list = config.get('MYSQL', 'tables');
    tables = tables_csv_list.strip().split(',')

    # check in dynamodb which tables are there..
    #current_tables = list_existing_tables();
    #print("current schemas in dynamodb: " + str(current_tables))
    
    # open the connection
    cnx = mysql.connector.connect(user=config.get("MYSQL", "db_user"),
                                  database=config.get("MYSQL", "db_name"),
                                  password=config.get("MYSQL", "db_password"),
                                  host=config.get("MYSQL", "db_host"),
                                  port=config.get("MYSQL", "db_port"))
    cursor = cnx.cursor()
    
    for t in tables:

        print "processing table: " + t
        array_attr_definitions = []
        query = ("SELECT * from " + t)

        # execute the query
        cursor.execute(query)

        # build the attribute definitions based on the
        # for col in cols:
        #    attribute_definition = {'AttributeName': col, 'AttributeType': 'S'}
        #    array_attr_definitions.append(attribute_definition)

        # get attribute name for the table - this should be set up in the config
        attribute_name=config.get("MYSQL", t)

        ks = [{'AttributeName':attribute_name, 'KeyType':'HASH'}]
        attributes = [{'AttributeName': attribute_name, 'AttributeType': 'S'}]

        # call dynamodb to create the schema
        response = create_schema(t, ks, attributes)

        # print "Attribute definitions: " + json.dumps(array_attr_definitions)
        # print "Key schema: " + json.dumps(ks)

        cols = [i[0] for i in cursor.description]
        # Start importing data into the table now.
        rows = cursor.fetchall()
        print '# rows=' + str(len(rows))

        for index,row in enumerate(rows):
            print 'processing row #'+str(index)
            attrs = {}
            for index,col in enumerate(cols):
                attr_name = str(col)
                attr_value = str(row[index])
                if attr_value:
                    attrs[attr_name] = attr_value
            attrs_json = json.dumps(attrs)
            print attrs
            # create data in dynamodb.
            table = dynamodb.Table(t)
            print ("adding item to table: "+str(table))
            with table.batch_writer() as batch:
                batch.put_item(Item=attrs)
    # close the connection
    cnx.close()
    print '--end--'


if __name__ == "__main__":
    main()