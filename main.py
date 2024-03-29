import datetime
import json
import paho.mqtt.client as mqtt
import configparser
from time import sleep
import ssl
import uuid
import js2py
import psycopg2
import base64
import json
import boto3
from decimal import Decimal
from threading import Thread
from queue import Queue


# handle error
def error_str(rc):
    return '{} {}: {}'.format(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), rc, mqtt.error_string(rc))


# connect status
def on_connect(unusued_client, unused_userdata, unused_flags, rc):
    print(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), 'on_connect', error_str(rc))
    if rc==0:
        print('connected')
        global connected
        connected = True
        

def publish_command(command, topic):
    client.publish(topic, command, qos=1)


def on_publish(unused_client, unused_userdata, unused_mid):
    print(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), 'on_publish')


# handle income message
def on_message(client, userdata, message):
    print(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), "message topic=", message.topic)
    messages_list.put(message)


# thread process
def handle_message(stop):
    while True:
        try:
            while not messages_list.empty():
                message = messages_list.get()
                # convert message body to json
                json_data = json.loads(str(message.payload.decode("utf-8")), parse_float=Decimal)

                ss = message.topic.split('/')
                schema_name = ss[0]
                prefix = ss[1]
                subtopic = ss[2]

                devEUI = get_item_from_dict('devEUI', json_data)
                # connect to mysql server
                db = psycopg2.connect(host=cp.get('Default', 'host'),
                                      user=cp.get('Default', 'user'),
                                      password=cp.get('Default', 'passwd'),
                                      database=cp.get('Default', 'db'),
                                      port=5432,
                                      options="-c search_path=dbo,%s" % schema_name)
                cursor = db.cursor()
                query = '''
                    SELECT d."id" as "deviceUID", "buildingFloorAreaName", "deviceApplicationEUI", "deviceApplicationKey", 
                    "deviceGatewayEUI", "deviceSerialNumber", "deviceAssetNumber", "deviceLatitude", "deviceLongitude", "deviceAltitude",
                    "deviceMACAddress", "devicePicture", "deviceLastPayloadReceived", "deviceCreatedDate", "deviceLastAccessDate", 
                    "deviceModelUID", "deviceModelName", "en_deviceModelDescription", "fr_deviceModelDescription", 
                    "deviceModelType", "deviceModelSupplierName", "deviceModelClassName", "deviceModelNetwork", "deviceModelCategory", 
                    "deviceModelDecoder", "deviceModelEncoder", "deviceModelValveCommand",
                    "deviceModelCreatedDate", "deviceModelLastAccessDate", a."id" as "accountUID", "accountName", "accountAddress", 
                    "accountAddress2", "accountCity", "accountState", "accountZipCode", "accountCountry",
                    "accountLatitude", "accountLongitude", "accountAltitude", "accountUrl", "accountStatus", "accountCreatedDate", 
                    "accountLastAccessDate", "buildingUID", "buildingName", "buildingAddress", "buildingAddress2",
                    "buildingCity", "buildingState", "buildingZipCode", "buildingCountry", "buildingLatitude", "buildingLongitude", 
                    "buildingAltitude", "buildingUrl", "buildingPicture", "buildingIcon", "buildingStatus",
                    "buildingMqttTopicPrefix", "buildingCreatedDate", "buildingLastAccessDate", "buildingFloorUID", "buildingFloorName", 
                    "buildingFloorDescription", "buildingFloorLastAccessDate", "buildingFloorAreaUID",
                    "buildingFloorAreaName", "buildingFloorAreaDescription", "buildingFloorAreaLastAccessDate"
                    from "%s"."Device" d
                    left join "%s"."DeviceModel" dm on d."deviceModel"=dm."id"
                    left join "%s"."BuildingFloorArea" bfa on d."buildingFloorArea"=bfa."id"
                    left join "%s"."BuildingFloor" bf on d."buildingFloor"=bf."id"
                    left join "%s"."Building" b on d."building"=b."id"
                    left join "%s"."Account" a on b."account"=a."id"
                    where LOWER("devEUI")=LOWER('%s')
                ''' % (schema_name, schema_name, schema_name, schema_name, schema_name, schema_name, devEUI)
                cursor.execute(query)
                db_data = cursor.fetchone()

                cursor.close()
                db.close()

                if db_data:
                    id = str(uuid.uuid4())
                    dt = datetime.datetime.utcnow()
                    timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')

                    # get decoded data
                    js_decoder = db_data[24]
                    if js_decoder:
                        datadecoded = str(get_decoded_data(js_decoder, get_item_from_dict('fPort', json_data), get_item_from_dict('data', json_data)))
                        datadecoded = datadecoded.replace("\'", "\"")
                        json_datadecoded = json.loads(datadecoded, parse_float=Decimal)
                        print(json_datadecoded, len(json_datadecoded))
                    else:
                        datadecoded = ''
                        json_datadecoded = {}

                    if subtopic == 'events':
                        db = psycopg2.connect(host=cp.get('Default', 'host'),
                                              user=cp.get('Default', 'user'),
                                              password=cp.get('Default', 'passwd'),
                                              database=cp.get('Default', 'db'),
                                              port=5432,
                                              options="-c search_path=dbo,%s" % schema_name)
                        cursor = db.cursor()


                        query = '''
                            UPDATE "%s"."Device" SET "deviceLastPayloadReceived"='%s' WHERE "id"=%d
                        ''' % (schema_name, timestamp, db_data[0])
                        cursor.execute(query)
                        db.commit()

                        # Insert event
                        if len(json_datadecoded) > 0:
                            if get_item_from_dict('water_leak', json_datadecoded):
                                query = '''
                                    SELECT "eventResolvedDate" FROM "%s"."Event" WHERE "device_id"=%d and "eventDescription"='Water leak detected' 
                                    ORDER BY "eventCreatedDate" DESC LIMIT 1
                                ''' % (schema_name, db_data[0])
                                cursor.execute(query)
                                values = cursor.fetchone()

                                if json_datadecoded['water_leak'] == 'leak':
                                    if values is None or values[0] is not None:
                                        query = '''
                                            INSERT INTO "%s"."Event" ("eventDescription", "eventStatus", "device_id", "eventCreatedDate") 
                                            VALUES ('%s', TRUE, %d, '%s')
                                        ''' % (schema_name, 'Water leak detected', db_data[0], timestamp)
                                        cursor.execute(query)
                                        db.commit()
                                else:
                                    if values and values[0] is None:
                                        query = '''
                                            UPDATE "%s"."Event" SET "eventResolvedDate"='%s' 
                                            WHERE "device_id"=%d AND "eventDescription"='Water leak detected' and "eventResolvedDate" IS NULL
                                        ''' % (schema_name, timestamp, db_data[0])
                                        cursor.execute(query)
                                        db.commit()

                            battery = get_item_from_dict('battery', json_datadecoded)
                            if battery:
                                query = '''
                                    SELECT "eventResolvedDate" FROM "%s"."Event" WHERE "device_id"=%d and "eventDescription"='Low Battery' 
                                    ORDER BY "eventCreatedDate" DESC LIMIT 1
                                ''' % (schema_name, db_data[0])
                                cursor.execute(query)
                                values = cursor.fetchone()

                                if battery < float(cp.get('Default', 'thres_Battery')):
                                    if values is None or values[0] is not None:
                                        query = '''
                                            INSERT INTO "%s"."Event" ("eventDescription", "eventStatus", "device_id", "eventCreatedDate") 
                                            VALUES ('%s', TRUE, %d, '%s')
                                        ''' % (schema_name, 'Low Battery', db_data[0], timestamp)
                                        cursor.execute(query)
                                else:
                                    if values and values[0] is None:
                                        query = '''
                                            UPDATE "%s"."Event" SET "eventResolvedDate"='%s' 
                                            WHERE "device_id"=%d AND "eventDescription"='Low Battery' AND "eventResolvedDate" IS NULL
                                        ''' % (schema_name, timestamp, db_data[0])
                                        cursor.execute(query)
                                        db.commit()

                                query = '''
                                    UPDATE "%s"."Device" SET "deviceBattery"=%d, "deviceBatteryUpdatedDate"='%s' WHERE "id"=%d
                                ''' % (schema_name, battery, timestamp, db_data[0])
                                cursor.execute(query)
                                db.commit()

                            dout1 = get_item_from_dict('dout1', json_datadecoded)
                            if dout1:
                                valveStatus = -1
                                if dout1 == 'on':
                                    valveStatus = 1
                                else:
                                    valveStatus = 0

                                query = '''
                                    UPDATE "%s"."Device" SET "deviceValveStatus"=%d WHERE "id"=%d
                                ''' % (schema_name, valveStatus, db_data[0])
                                cursor.execute(query)
                                db.commit()

                        # Get radio status
                        rxInfo = get_item_from_dict('rxInfo', json_data)
                        if rxInfo and len(rxInfo) > 0:
                            loRaSNR = get_item_from_dict('loRaSNR', rxInfo[0])
                            rssi = get_item_from_dict('rssi', rxInfo[0])

                            query = '''
                                SELECT "eventResolvedDate" FROM "%s"."Event" WHERE "device_id"=%d and "eventDescription"='Radio Lost' 
                                ORDER BY "eventCreatedDate" DESC LIMIT 1
                            ''' % (schema_name, db_data[0])
                            cursor.execute(query)
                            values = cursor.fetchone()

                            if loRaSNR and rssi:
                                if loRaSNR > float(cp.get('Default', 'thres_SNR')) or rssi < float(cp.get('Default', 'thres_RSSI')):
                                    if values is None or values[0] is not None:
                                        query = '''
                                            INSERT INTO "%s"."Event" ("eventDescription", "eventStatus", "device_id", "eventCreatedDate") 
                                            VALUES ('%s', TRUE, %d, '%s')
                                        ''' % (schema_name, 'Radio Lost', db_data[0], timestamp)
                                        cursor.execute(query)
                                        db.commit()
                                else:
                                    if values and values[0] is None:
                                        query = '''
                                            UPDATE "%s"."Event" SET "eventResolvedDate"='%s' 
                                            WHERE "device_id"=%d AND "eventDescription"='Radio Lost' AND "eventResolvedDate" IS NULL
                                        ''' % (schema_name, timestamp, db_data[0])
                                        cursor.execute(query)
                                        db.commit()

                            if loRaSNR:
                                query = '''
                                    UPDATE "%s"."Device" SET "deviceSNR"=%d, "deviceSNRUpdatedDate"='%s' WHERE "id"=%d
                                ''' % (schema_name, loRaSNR, timestamp, db_data[0])
                                cursor.execute(query)
                                db.commit()

                            if rssi:
                                query = '''
                                    UPDATE "%s"."Device" SET "deviceRSSI"=%d, "deviceRSSIUpdatedDate"='%s' WHERE "id"=%d
                                ''' % (schema_name, rssi, timestamp, db_data[0])
                                cursor.execute(query)
                                db.commit()

                        # Send close command
                        query = '''
                            SELECT "to_device_id", "deviceModelValveCommand", "devEUI" from "%s"."Device_deviceRelationship" ddr 
                            left join "%s"."Device" d on ddr.to_device_id =d.id 
                            left join "%s"."DeviceModel" dm on d."deviceModel" = dm.id
                            where from_device_id=%d
                        ''' % (schema_name, schema_name, schema_name, db_data[0])
                        cursor.execute(query)
                        commands_data = cursor.fetchall()

                        if commands_data and len(commands_data) > 0 and get_item_from_dict('water_leak', json_datadecoded) and json_datadecoded['water_leak'] == 'leak':
                            for commands in commands_data:
                                if commands and commands[1]:
                                    close_command = ''
                                    command_list = commands[1].splitlines()
                                    for command in command_list:
                                        if command.find('CLOSE') > -1:
                                            idx1 = command.find('{')
                                            idx2 = command.find('}')
                                            close_command = command[idx1:idx2+1]

                                            # insert valve close
                                            query = '''
                                                INSERT INTO "%s"."ValveClose" ("valveCloseDate", "device_id") VALUES ('%s', %d)
                                            ''' % (schema_name, timestamp, commands[0])
                                            cursor.execute(query)
                                            db.commit()

                                            publish_command(close_command, schema_name + '/' + prefix + '/commands/' + commands[2])

                                            # add Valve Closed event
                                            query = '''
                                                INSERT INTO "%s"."Event" ("eventDescription", "eventStatus", "device_id", "eventCreatedDate") 
                                                VALUES ('%s', TRUE, %d, '%s')
                                            ''' % (schema_name, 'Valve Closed', commands[0], timestamp)
                                            cursor.execute(query)
                                            db.commit()

                        cursor.close()
                        db.close()

                    # create new item to be inserted into dynamodb
                    new_item = {
                        'id': id, 'timestamp': int(dt.timestamp()), 'topic': message.topic,
                        'applicationID': get_item_from_dict('applicationID', json_data),
                        'applicationName': get_item_from_dict('applicationName', json_data),
                        'data': get_item_from_dict('data', json_data),
                        'datadecoded': datadecoded,
                        'devEUI': devEUI,
                        'deviceName': get_item_from_dict('deviceName', json_data),
                        'fCnt': get_item_from_dict('fCnt', json_data),
                        'fPort': get_item_from_dict('fPort', json_data),
                        'rxInfo': get_item_from_dict('rxInfo', json_data),
                        'txInfo': get_item_from_dict('txInfo', json_data),
                        'dataRate': get_item_from_dict('dataRate', json_data),
                        'frequency': get_item_from_dict('frequency', json_data),
                        'deviceUID': db_data[0], 'buildingFloorAreaName': db_data[1],
                        'deviceApplicationEUI': db_data[2], 'deviceApplicationKey': db_data[3],
                        'deviceGatewayEUI': db_data[4], 'deviceSerialNumber': db_data[5],
                        'deviceAssetNumber': db_data[6], 'deviceLatitude': str(db_data[7]),
                        'deviceLongitude': str(db_data[8]), 'deviceAltitude': str(db_data[9]),
                        'deviceMACAddress': db_data[10], 'devicePicture': db_data[11],
                        'deviceLastPayloadReceived': datetostring(db_data[12]), 'deviceCreatedDate': datetostring(db_data[13]),
                        'deviceLastAccessDate': datetostring(db_data[14]), 'deviceModelUID': db_data[15],
                        'deviceModelName': db_data[16],
                        'deviceModelDescription': db_data[17], 'deviceModelType': db_data[19],
                        'deviceModelSupplierName': db_data[20], 'deviceModelClassName': db_data[21],
                        'deviceModelNetwork': db_data[22], 'deviceModelCategory': db_data[23],
                        'deviceModelDecoder': db_data[24], 'deviceModelEncoder': db_data[25],
                        'deviceModelValveCommand': db_data[26], 'deviceModelCreatedDate': datetostring(db_data[27]),
                        'deviceModelLastAccessDate': datetostring(db_data[28]),
                        'accountUID': db_data[29], 'accountName': db_data[30],
                        'accountAddress': db_data[31], 'accountAddress2': db_data[32],
                        'accountCity': db_data[33], 'accountState': db_data[34],
                        'accountZipCode': db_data[35], 'accountCountry': db_data[36],
                        'accountLatitude': str(db_data[37]),
                        'accountLongitude': str(db_data[38]), 'accountAltitude': str(db_data[39]),
                        'accountUrl': db_data[40], 'accountStatus': db_data[41],
                        'accountCreatedDate': datetostring(db_data[42]),
                        'accountLastAccessDate': datetostring(db_data[43]), 'buildingUID': db_data[44],
                        'buildingName': db_data[45], 'buildingAddress': db_data[46], 'buildingAddress2': db_data[47],
                        'buildingCity': db_data[48], 'buildingState': db_data[49], 'buildingZipCode': db_data[50],
                        'buildingCountry': db_data[51], 'buildingLatitude': str(db_data[52]),
                        'buildingLongitude': str(db_data[53]), 'buildingAltitude': str(db_data[54]),
                        'buildingUrl': db_data[55], 'buildingPicture': db_data[56], 'buildingIcon': db_data[57],
                        'buildingStatus': db_data[58], 'buildingMqttTopicPrefix': db_data[59],
                        'buildingCreatedDate': datetostring(db_data[60]), 'buildingLastAccessDate': datetostring(db_data[61]),
                        'buildingFloorUID': db_data[62], 'buildingFloorName': db_data[63],
                        'buildingFloorDescription': db_data[64], 'buildingFloorLastAccessDate': datetostring(db_data[65]),
                        'buildingFloorAreaUID': db_data[66], 'buildingFloorAreaName': db_data[67],
                        'buildingFloorAreaDescription': db_data[68],
                        'buildingFloorAreaLastAccessDate': datetostring(db_data[69])
                    }

                    dynamodb = boto3.resource('dynamodb', aws_access_key_id=cp.get('Default', 'aws_access_key_id'), aws_secret_access_key=cp.get('Default', 'aws_secret_access_key'), region_name=cp.get('Default', 'region_name'))
                    table = dynamodb.Table('tfmqtt_stream')
                    res = table.put_item(Item=new_item)
                    print(res)
        except Exception as err:
            print('error', err)

        sleep(1)
        if stop():
            break


# get decoded data from javascript decode function
def get_decoded_data(js_decoder, port, data):
    context = js2py.EvalJs()  
    base64_bytes = data.encode('utf-8')
    data_bytes = base64.b64decode(base64_bytes)
    
    context.execute(js_decoder)

    input_arr = []
    for el in data_bytes:
        input_arr.append(el)

    return context.Decode(port, input_arr)


def datetostring(val):
    if val:
        return val.strftime('%Y-%m-%d %H:%M:%S')
    else:
        return ''


def get_item_from_dict(key, jd):
    if key in jd:
        return jd[key]
    else:
        return ''


def main():
    awshost = cp.get('Default', 'awshost')
    awsport = 8883

    while True:
        try:
            global connected
            connected = False

            # connect to mysql server and get topics
            db = psycopg2.connect(host=cp.get('Default', 'host'),
                                  user=cp.get('Default', 'user'),
                                  password=cp.get('Default', 'passwd'),
                                  database=cp.get('Default', 'db'),
                                  port=5432,)
            cursor = db.cursor()
            
            query = '''
                SELECT "schema_name" from "organization_organization"
            '''

            cursor.execute(query)
            schema_data = cursor.fetchall()

            cursor.close()
            db.close()

            topics = []
            for schema_datum in schema_data:
                schema_name = schema_datum[0]
                if schema_name != 'public':
                    db = psycopg2.connect(host=cp.get('Default', 'host'),
                              user=cp.get('Default', 'user'),
                              password=cp.get('Default', 'passwd'),
                              database=cp.get('Default', 'db'),
                              port=5432,
                              options="-c search_path=dbo,%s" % schema_name)
                    cursor = db.cursor()
                    query = '''
                        SELECT "buildingMqttTopicPrefix" from "%s"."Building"
                    ''' % schema_name

                    cursor.execute(query)
                    db_data = cursor.fetchall()

                    cursor.close()
                    db.close()

                    for item in db_data:
                        if item[0]:
                            topics.append((schema_name+'/'+item[0].strip()+'/#', 0))

            if len(topics) == 0:
                sleep(60)
            else:
                client.connect(awshost, awsport, keepalive=60)

                client.loop_start()

                while not connected:
                    sleep(0.1)

                stop_threads = False
                thread = Thread(target=handle_message, args=(lambda: stop_threads, ))
                thread.start()

                client.subscribe(topics)

                sleep(3600)
                client.loop_stop()
                stop_threads = True
                thread.join()

        except Exception as err:
            print('error')

        finally:
            sleep(10)


messages_list = Queue()

# Read config
cp = configparser.RawConfigParser()
cp.read('setting.cfg')

# Initialize mqtt client
caPath = cp.get('Default', 'caPath')
certPath = cp.get('Default', 'certPath')
keyPath = cp.get('Default', 'keyPath')
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.tls_set(caPath, certfile=certPath, keyfile=keyPath, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)



connected = False


if __name__ == '__main__':
    main()
