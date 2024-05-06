import sys, os, requests, datetime, re, logging, logging.handlers
import paho.mqtt.publish as publish
import paho.mqtt.client as mqtt
import pytz
import configparser
from time import sleep

# Debug mode.
DEBUG = 0


class PowerstationMqtt():
    def init(self):
        self.powerstation_hostname = os.environ.get('powerstation_hostname','')
        self.powerstation_instance = os.environ.get('powerstation_instance','')
        self.powerstation_milliwatt = os.environ.get('powerstation_milliwatt', '')
        if self.powerstation_milliwatt == 'true':
            self.powerstation_divisor = 1000.0
        else:
            self.powerstation_divisor = 1.0
        
        self.mqtt_client_id = os.environ.get('mqtt_client_id','')
        self.mqtt_host = os.environ.get('mqtt_client_host','')
        self.mqtt_port = int(os.environ.get('mqtt_client_port',''))
        self.mqtt_topic = os.environ.get('mqtt_client_root_topic','')
        self.mqtt_qos = int(os.environ.get('mqtt_qos',''))
        self.mqtt_retain = eval(os.environ.get('mqtt_retain',''))
        
        if eval(os.environ.get('mqtt_auth','')):
            self.mqtt_username = os.environ.get('mqtt_username','')
            self.mqtt_password = os.environ.get('mqtt_password','')
            self.mqtt_auth = { "username": os.environ.get('mqtt_username',''), "password": os.environ.get('mqtt_password','') }
        else:
            self.mqtt_auth = None

        logging.basicConfig(stream=sys.stdout, format='%(asctime)s: %(name)s %(levelname)s: %(message)s')
        logger = logging.getLogger(__name__)
        logger.level = logging.INFO
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler = logging.handlers.RotatingFileHandler("/log/powerstation-mqtt-" + self.powerstation_instance + ".log", maxBytes=10000000, backupCount=4)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        statuslogger = logging.getLogger("status")
        statuslogger.level = logging.INFO
        statushandler = logging.handlers.RotatingFileHandler("/log/powerstation-mqtt-status-" + self.powerstation_instance + ".log", maxBytes=1000000, backupCount=2)
        statushandler.setFormatter(formatter)
        statuslogger.addHandler(statushandler)


        self.logger = logger
        self.statuslogger = statuslogger

        logger.info("initialized")
        statuslogger.info("initialized") 
        
    def on_message(self, client, userdata, message):
        try:
            if (message.topic.startswith("$SYS/")):
                self.watchdog = 0
            else:
                self.watchdog = 0
                port = message.topic.split('/')[-1]
                value = str(message.payload.decode("utf-8"))
                state = "0"

                if value == "ON" or value == "1":
                    state = "1"
            
                url = 'http://' + self.powerstation_hostname + '?cmd=200&json={"port":' + port +',"state":' + state + '}' 
                self.logger.debug(url)

                requests.get(url)
        except Exception as e:
            self.logger.warning(e)
            pass
        
 
    def run(self):
        self.logger.info("running")
        self.statuslogger.info("running")

        
        lastRun = datetime.datetime.utcnow()
        lastEval = datetime.datetime.utcnow()
        watt = [0.0, 0.0, 0.0, 0.0, 0.0 ,0.0, 0.0]
        kwh = [0.0, 0.0, 0.0, 0.0, 0.0 ,0.0, 0.0]
        kwhSent = [0.0, 0.0, 0.0, 0.0, 0.0 ,0.0, 0.0]
        switch = [0,0,0,0,0,0]
        lastSentTime = datetime.datetime.fromordinal(1) 

        while True:
            self.watchdog = 0

            sendClientId = self.mqtt_client_id + "Send"
            client = mqtt.Client(client_id=sendClientId)
            self.client = client
            client.enable_logger(logger=self.logger)
            if self.mqtt_username:
                self.logger.info("apply credentials")
                client.username_pw_set(self.mqtt_username, self.mqtt_password)

            client.reconnect_delay_set(min_delay=1, max_delay=120)
            client.on_message = self.on_message
            
            client.connect(self.mqtt_host, port=self.mqtt_port, keepalive=60, bind_address="")
            client.loop_start()
           
            client.subscribe(self.mqtt_topic + "send/#", qos=1)
            client.subscribe("$SYS/broker/uptime", qos=1)

            while self.watchdog < 60:
                self.watchdog = self.watchdog + 1
                try:
                    while True:
                        now = datetime.datetime.utcnow()
                        delta = (now - lastRun).total_seconds() 
                        if delta >= 1:
                            lastRun = now
                            break
                        sleep(0.1)
                
                    self.statuslogger.info("looping")
                    self.logger.debug("Requesting Data")

                    response = requests.get("http://" + self.powerstation_hostname + "?cmd=511", timeout=10.0)
                    now = datetime.datetime.utcnow()
                    deltaEval = (now - lastEval).total_seconds()
                    lastEval = now
                    deltaSent = (now - lastSentTime).total_seconds()

                    jsonData = response.json()
                    if 'data' not in jsonData:
                        self.logger.warning("Data not found")
                        break

                    data = jsonData["data"]
                    self.logger.debug("Processing  Data")
                    sWatt = data["watt"]
                    rWatt = [0.0, 0.0, 0.0, 0.0, 0.0 ,0.0, 2.0]
                    rSwitch = data["switch"]
                    rDivisor = 

                    for x in range(6):
                        rWatt[x] = float(sWatt[x]) / self.powerstation_divisor;
                        kwh[x] = kwh[x] + rWatt[x] * deltaEval / 3600000.0
                        rWatt[6] = rWatt[6] + rWatt[x]
            
                    kwh[6] = kwh[6] + rWatt[x] * deltaEval / 3600000.0

                    if deltaSent > 600:
                        for x in range(7):
                            topic = self.mqtt_topic + "Port" + str(x)
                            
                            if (x != 6):
                                switch[x] = rSwitch[x]
                                state = "ON"
                                if int(switch[x]) == 0:
                                    state = "OFF"
                                self.client.publish(topic + "/switch", payload=state, qos=self.mqtt_qos, retain=self.mqtt_retain)
                            else:
                                topic = self.mqtt_topic + "Total"

                            watt[x] = rWatt[x]
                            self.client.publish(topic + "/watt", payload=watt[x], qos=self.mqtt_qos, retain=self.mqtt_retain)
                            kwhSent[x] = kwh[x]
                            self.client.publish(topic + "/kWh", payload=kwhSent[x], qos=self.mqtt_qos, retain=self.mqtt_retain)
                        lastSentTime = now
                    else:
                        for x in range(7):
                            topic = self.mqtt_topic + "Port" + str(x)
                            if (x != 6):
                                if switch[x] != rSwitch[x]:
                                    switch[x] = rSwitch[x]
                                    state = "ON"
                                    if int(switch[x]) == 0:
                                        state = "OFF"
                                    self.client.publish(topic + "/switch", payload=state, qos=self.mqtt_qos, retain=self.mqtt_retain)
                            else:
                                topic = self.mqtt_topic + "Total"
                            if abs(watt[x] - rWatt[x]) >= 0.5:
                                watt[x] = rWatt[x]
                                self.client.publish(topic + "/watt", payload=watt[x], qos=self.mqtt_qos, retain=self.mqtt_retain)
                            if abs(kwh[x] - kwhSent[x]) >= 0.01:
                                kwhSent[x] = kwh[x]
                                self.client.publish(topic + "/kWh", payload=kwhSent[x], qos=self.mqtt_qos, retain=self.mqtt_retain)
                    
                except Exception as e:
                    self.logger.warning(e)
                    pass

            self.logger.warning("watchdog triggered, restarting mqtt")
            client.disconnect()


d = PowerstationMqtt()
d.init()
d.run()
