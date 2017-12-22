import pika
import json
import datetime
import sys
import os
from collections import deque

# skript inicializuje promenne, pripoji se k RabbitMQ a odebira zpravy, zapisuje do tmp souboru 
# testuje z tmp souboru, kdy naposledy dobehlo OK, vrati vysledek suitable pro Nagios


class Monitor:
    def __init__(self, *args, **kwargs):
    
        self.username = 'admin'
        self.password = 'kgnv5*wdVRwacs'
        self.host = 'rabbitmq01.core.ignum.cz'
        self.port = 5672
        self.vhost = '/ipas_test'
        self.queue = 'ipas-test-monitor-commands'
        self.cmdlog_id = ''
        self.tmpfile = '/tmp/soubor'


    def started_case(self, msg):
        load = json.loads(msg)
        self.cmdlog_id = load['fields']['cmdlog_id']    
  
        
    def finished_case(self, msg):
        load = json.loads(msg)  
        if load['fields']['cmdlog_id'] != self.cmdlog_id:
            return
            
        last_run = datetime.datetime.now()
        with open(self.tmpfile, 'r+') as f:
            buffer1 = deque(f, maxlen=30)
        buffer1.append(str(last_run) + " " + str(load['fields']['result']['code']) + "\n")
        with open(self.tmpfile, 'w+') as f:
            f.writelines(buffer1)
    
        
    def callback(self, ch, method, properties, body):
        msg = str(body, 'utf-8')
        if 'command.started' in msg and 'ProcessPendingActions' in msg:
            self.started_case(msg)
        if 'command.finished' in msg:
            self.finished_case(msg)  
            
    #def end_consuming(self):
        #self.channel.stop_consuming()
        #self.connection.close()      
   
        
    def connect(self):
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(self.host,
                                               self.port,
                                               self.vhost,
                                               credentials)
        self.connection = pika.BlockingConnection(parameters)          
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue, durable=True)             
        #self.connection.add_timeout(deadline=5, callback_method=self.end_consuming)
        
        try:
            self.channel.basic_consume(self.callback,
                              queue=self.queue,
                              no_ack=True)
            self.channel.start_consuming()  
        
        except KeyboardInterrupt:
            self.channel.stop_consuming()
            self.connection.close()
            
            
    def check_nagios(self):
        if not os.path.isfile(self.tmpfile) or os.stat(self.tmpfile).st_size == 0:
            print('File doesn\'t exist or is empty')
            exit(2)
        with open(self.tmpfile, 'r') as f:
            data = f.readlines()
        for line in reversed(data):   
            date, time, code = line.split(' ') 
            if int(code) is 0: 
                break
                
        last_run = datetime.datetime.strptime(date+' '+time, '%Y-%m-%d %H:%M:%S.%f')
        if datetime.datetime.now() - last_run <= datetime.timedelta(minutes=2):
            print('OK')
            exit(0)
        if datetime.datetime.now() - last_run > datetime.timedelta(minutes=2) and datetime.datetime.now() - last_run <= datetime.timedelta(minutes=10): 
            print('/ipas_test -> queue ipas-test-monitor-commands -> command.*: More then XXX minutes without OK exit code!')
            exit(1)   
        if datetime.datetime.now() - last_run > datetime.timedelta(minutes=10): 
            print('/ipas_test -> queue ipas-test-monitor-commands -> command.*: More then 10XXX minutes without OK exit code!')
            exit(2)         
        

instance = Monitor()
for arg in sys.argv:
    if arg == 'connect':
        instance.connect()
    if arg == 'nagios': 
        instance.check_nagios()
