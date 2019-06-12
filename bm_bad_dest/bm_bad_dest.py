#!/usr/bin/env python3

import paho.mqtt.client as mqtt
import subprocess
import time
from datetime import date, datetime
from configparser import SafeConfigParser
import logging
import logging.handlers
import json
import os


exit_me = False

logfile= "logfile.txt"
parser = SafeConfigParser()
parser.read('config.ini')

# Constant

PREFIX = "Master/"+ str(parser.get('mqtt','serverid')) +"/Session/#"

my_name = parser.get('mqtt', 'clientname')
output_format = my_name+' %(message)s'

# Set up Logger object
logger = logging.getLogger(my_name)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s '+output_format)

# Set up a handler for pushing to syslog
# This line should be used for a properly listening syslogd
#handler = logging.handlers.SysLogHandler(facility=logging.handlers.SysLogHandler.LOG_DAEMON, address=('localhost', 514))
handler = logging.handlers.SysLogHandler(facility=logging.handlers.SysLogHandler.LOG_DAEMON, address='/dev/log')
# Change to our format
formatter = logging.Formatter(output_format)
handler.setFormatter(formatter)
logger.addHandler(handler)


def sendmessage(client, msg_type, src, dest, msg):
    topic = "Master/" + str(parser.get('mqtt','serverid')) + "/Outgoing/" + msg_type + "/" + str(src) + "/" + str(dest)
    client = mqtt.Client("noname", clean_session=True)
    client.connect(parser.get('mqtt','serverfqdn'))
    time.sleep(1)
    client.publish(topic,msg.encode("utf-16-le"))
    client.disconnect()
    return

def sendprivatemessage(client, destination, msg):
    # Type, privateID,
    sendmessage(client, "Message", 272990, destination, msg)
    return 

def on_publish(client,userdata,result):
    print("message sent")
    pass


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    logger.debug("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    topic_list = []
    for item in parser.get('mqtt', 'topics').split(','):
        topic_list.append((item.lstrip(), 0))
    logger.info("Topic list is: %s" % topic_list)
    client.subscribe(topic_list)
    logger.info("Subscribed")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, bad_destinations, msg):
    #global exit_me
    #print("Message is " + msg)
    
    if ("Master/"+parser.get('mqtt','serverid')+"/Incoming/Report" in str(msg.topic)):
        i = int.from_bytes(msg.payload, byteorder='big')
        if (i == 8) :
            print("Last message to " + str(msg.topic) + " Delivered")
        return
   
    m_decode=str(msg.payload.decode("utf-8","ignore"))
    m_in=json.loads(m_decode)
    
    if "Event" in m_in:
        my_type = m_in.get('Event')
        if (my_type == "Session-Start" or 
            my_type == "Session-Stop"):

            if (2729999 > m_in.get('SourceID') and 
                2720000 < m_in.get('SourceID')):
                with open (parser.get('logging', 'filename'), 'a+') as outfile:
                    outfile.write(datetime.now().strftime("%Y%m%d-%H%M%S") + " : " + str(m_in.get('SourceID')) + " to " + str(m_in.get('DestinationID')) + " - " + str(my_type) + "\n")
         
        if ( my_type == "Session-Stop" ):
            src=m_in.get('SourceID')
            dest=m_in.get('DestinationID')

            # if the Source DMR ID is less than 1000000 or if the Link Type (from Script/Common/Core.lua) is over the network
            if ( src < 1000000 or int(m_in.get('LinkType') == 2)):
                #do nothing
                return

	        # Seriously, update your codeplug  
            if(dest == 5057):
	            now = datetime.now()
	            # no real reason.. just higher than 3600
	            delta = 10000

	            if src in bad_destinations:
	                date_time_obj = datetime.strptime(bad_destinations[src],'%Y%m%d-%H%M%S')
	                delta = (now - date_time_obj).total_seconds()
	            
	            if delta > 3600:
	                bad_destinations[src] = now.strftime("%Y%m%d-%H%M%S")
	                logger.info("Sending message to " + str(src))
	                msg = "Destination Error. 5057 no longer works, use 272999 on this server\n"
	                sendprivatemessage(client, src, msg)

	                with open (parser.get('logging', 'filename'), 'a+') as outfile:
	                    outfile.write(datetime.now().strftime("%Y%m%d-%H%M%S") + " : " + str(src) + " (" + msg + ")\n")
	            # Write out the new file
	            with open ('bad_destinations.json', "w" ) as outfile:
	                json.dump(bad_destinations, outfile)
	        
            # 
            if ( dest == 272):
	            msg = "Destination Error, 272 is not in use, please use 2722 instead"
	            sendprivatemessage(client, src, msg)
	            logger.info("Message re 272 sent to: " + str(src))

       
def main():
    exists = os.path.isfile(parser.get('logging', 'bad_destinations'))
    if exists:
        with open(parser.get('logging', 'bad_destinations')) as json_file:
            print("Read"+parser.get('logging', 'bad_destinations'))
            bad_destinations = json.load(json_file)
            if bad_destinations:
                print(json.dumps(bad_destinations, indent=2))
    else:
        bad_destinations = {}
 
    client = mqtt.Client(parser.get('mqtt', 'clientname'), userdata=bad_destinations,
                         clean_session=True)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(parser.get('mqtt', 'serverfqdn'),
                   int(parser.get('mqtt', 'port')), 60)

    # Loop forever
    try:
        client.loop_forever()
    # Catches SigINT
    except KeyboardInterrupt:
        #global exit_me
        #exit_me = True
        client.disconnect()
        logger.info("Exiting main thread")
        time.sleep(2.0)

if __name__ == '__main__':
    main()

