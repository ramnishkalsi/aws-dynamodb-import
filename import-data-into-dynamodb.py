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
def create_schema(table_name):
    attribute_name = config.get("MYSQL", table_name)

    ks = [{'AttributeName': attribute_name, 'KeyType': 'HASH'}]
    attributes = [{'AttributeName': attribute_name, 'AttributeType': 'S'}]
    pt = {'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}

    print "Attribute definitions: " + json.dumps(attributes)
    print "Key schema: " + json.dumps(ks)
    try:
        table = dynamodb_client.create_table(TableName=table_name,
                                             KeySchema=ks,
                                             AttributeDefinitions=attributes,
                                             ProvisionedThroughput=pt)

        # table creation is an async operation, so wait while it is created.
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        print ("table created", table)
        return table
    except:  # catch all exceptions
        print "Unexpected error:", sys.exc_info()


def build_state_info(cursor, state_id):
    # print "building state info"
    # State info
    cursor.execute('Select info_head, info_desc from state_info where state_id=' + str(state_id))
    rows_state_information = cursor.fetchall()
    attributes_information = {}
    for index3, row3 in enumerate(rows_state_information):
        attr_name = row3[0]
        attr_value = row3[1]
        if attr_value:
            attributes_information[attr_name] = attr_value

    return attributes_information


def main():
    print "--start--"

    tables_csv_list = config.get('MYSQL', 'tables');
    tables = tables_csv_list.strip().split(',')

    # open the connection
    cnx = mysql.connector.connect(user=config.get("MYSQL", "db_user"),
                                  database=config.get("MYSQL", "db_name"),
                                  password=config.get("MYSQL", "db_password"),
                                  host=config.get("MYSQL", "db_host"),
                                  port=config.get("MYSQL", "db_port"))
    cursor = cnx.cursor()
    
    for t in tables:

        # print "processing table: " + t
        array_attr_definitions = []
        query = ("SELECT * from " + t)

        # execute the query
        cursor.execute(query)

        # Call dynamodb to create the schema

        response = create_schema(t)

        cols = [i[0] for i in cursor.description]

        # Start importing data into the table now.
        rows = cursor.fetchall()

        # print '# rows=' + str(len(rows))

        for index1, row in enumerate(rows):
            # print 'processing row #'+str(index1)
            state_id = row[0]

            # Core attributes
            attributes = {}
            for index2,col in enumerate(cols):
                attr_name = str(col)
                attr_value = str(row[index2])
                if attr_value:
                    attributes[attr_name] = attr_value
            attributes_json = json.dumps(attributes)

            # state info
            attributes["state_information"] = build_state_info(cursor, state_id)

            attributes_json = json.dumps(attributes)
            print attributes_json

            # uncomment below when data is ready, create data in dynamodb.
            table = dynamodb.Table(t)

            print ("adding item to table: "+str(table))

            with table.batch_writer() as batch:
                batch.put_item(Item=attributes)

    # close the connection
    cnx.close()
    print '--end--'


if __name__ == "__main__":
    main()