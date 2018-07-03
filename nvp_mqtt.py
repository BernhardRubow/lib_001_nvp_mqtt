
import paho.mqtt.client as mqtt
import ssl

class nvp_mqtt:

  def __init__(self, **kwargs):
      self.set_initial_values(kwargs)

    def create_client(self, message_handler=None, on_connect=None, on_disconnect=None):
        self.check_configuration()

        self.__client = mqtt.Client(self.clientId)
        self.__client.username_pw_set(self.username, self.password)

        if self.ssl == 1:
            self.__client.tls_set_context(self.create_ssl_context())

        self.__client.will_set(self.last_will_topic, 'offline', retain=True)

        if message_handler is not None:
            self.__client.on_message = message_handler
            
        if on_connect is not None:
            self.__client.on_connect = on_connect
            
        if on_disconnect is not None:
            self.__client.on_disconnect = on_disconnect


  def connect(self):
      self.__client.connect(self.broker_address, self.port, 60)
      self.__client.publish(self.last_will_topic, "online", 1, retain=True)


  def create_ssl_context(self):
      context = ssl.create_default_context()
      return context


  def loop_start(self):
      self.__client.loop_start()


  def loop_stop(self):
      self.__client.loop_stop()


  def subscribe(self, topic):
      self.__client.subscribe(topic)


  def publish(self, topic, payload, qos=0):
      self.__client.publish(topic, payload, qos)


  def raise_config_error_msg(self, msg):
      message = "Configuration Error: " + msg
      raise RuntimeError(message)


  def check_configuration(self):
      if self.broker_address == '':
          self.raise_config_error_msg("broker address not set")

      if self.port == 0:
          self.raise_config_error_msg("port not set")

      if self.username == "":
          self.raise_config_error_msg("username not set")

      if self.password == "":
          self.raise_config_error_msg("password not set")


  def set_initial_values(self, kwargs):
      self.broker_address = ''
      self.port = 0
      self.username = ''
      self.password = ''
      self.clientId = ''
      self.last_will_topic = ''
      self.__client = None

      if 'broker_address' in kwargs:
          self.broker_address = kwargs['broker_address']

      if 'port' in kwargs:
        self.port = kwargs['port']

      if 'username' in kwargs:
          self.username = kwargs['username']

      if 'password' in kwargs:
          self.password = kwargs['password']

      if 'last_will_topic' in kwargs:
          self.last_will_topic = kwargs['last_will_topic']

      if 'clientId' in kwargs:
          self.clientId = kwargs['clientId']

      if 'ssl' in kwargs:
          self.ssl = kwargs['ssl']

      
