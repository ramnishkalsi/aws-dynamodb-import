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


def main():
    print "--start--"

    tables_csv_list = config.get('MYSQL', 'tables');
    tables = tables_csv_list.strip().split(',')

    # check in dynamodb which tables are there..
    current_tables = list_existing_tables();
    print("current schemas in dynamodb: " + str(current_tables))
    
    # open the connection
    cnx = mysql.connector.connect(user=config.get("MYSQL", "db_user"),
                                  database=config.get("MYSQL", "db_name"),
                                  password=config.get("MYSQL", "db_password"))
    cursor = cnx.cursor()
    
    for t in tables:

        print "querying " + t

        array_attr_definitions = []
        query = ("SELECT * from " + t)
        
        # execute the query
        cursor.execute(query)

        cols = [i[0] for i in cursor.description]
        
        for col in cols:
            attribute_definition = {'AttributeName': col, 'AttributeType': 'S'}
            array_attr_definitions.append(attribute_definition)

        # table_json.append({"AttributeDefinitions" : array_attr_definitions})
        pt = {'ReadCapacityUnits': 5,'WriteCapacityUnits': 5}

        ks = [{'AttributeName':'country_id', 'KeyType':'HASH'}]
        attrs = [{'AttributeName': 'country_id', 'AttributeType': 'S'}]

        # print "Attribute definitions: " + json.dumps(array_attr_definitions)
        # print "Key schema: " + json.dumps(ks)
        try:
            table = dynamodb_client.create_table(TableName=t,KeySchema=ks,AttributeDefinitions=attrs,ProvisionedThroughput=pt )
            print("Table status:", str(table))
        except: # catch all exceptions
            print "Unexpected error:", sys.exc_info()

        # Start importing data into the table now.
        rows = cursor.fetchall()
        print '# rows=' + str(len(rows))

        for index,row in enumerate(rows):
            print 'processing row #'+str(index)
            attrs = {}
            for index,col in enumerate(cols):
                attr_name = str(col)
                attr_value = str(row[index])
                attrs[attr_name] = attr_value
            attrs_json = json.dumps(attrs)
            print attrs
            # create data in dynamodb.
            table = dynamodb.Table(t)
            print ("adding item to table: "+str(table))
            table.put_item(Item=attrs)

    
    # close the connection
    cnx.close()
    print '--end--'


if __name__ == "__main__":
    main()