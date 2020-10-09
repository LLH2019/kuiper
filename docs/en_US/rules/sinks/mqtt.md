# MQTT action

The action is used for publish output message into a MQTT server. 

| Property name      | Optional | Description                                                  |
| ------------------ | -------- | ------------------------------------------------------------ |
| server             | false    | The broker address of the mqtt server, such as ``tcp://127.0.0.1:1883`` |
| topic              | false    | The mqtt topic, such as ``analysis/result``                  |
| clientId           | true     | The client id for mqtt connection. If not specified, an uuid will be used |
| protocolVersion    | true     | 3.1 (also refer as MQTT 3) or 3.1.1 (also refer as MQTT 4).  If not specified, the default value is 3.1. |
| qos                | true     | The QoS for message delivery. Only int type value 0 or 1 or 2.                                |
| username           | true     | The user name for the connection.                            |
| password           | true     | The password for the connection.                             |
| certificationPath  | true     | The certification path. It can be an absolute path, or a relative path. If it is an relative path, then the base path is where you excuting the ``server`` command. For example, if you run ``bin/server`` from ``/var/kuiper``, then the base path is ``/var/kuiper``; If you run ``./server`` from ``/var/kuiper/bin``, then the base path is ``/var/kuiper/bin``. |
| privateKeyPath     | true     | The private key path. It can be either absolute path, or relative path. For more detailed information, please refer to ``certificationPath``. |
| insecureSkipVerify | true     | If InsecureSkipVerify is ``true``, TLS accepts any certificate presented by the server and any host name in that certificate.  In this mode, TLS is susceptible to man-in-the-middle attacks. The default value is ``false``. The configuration item can only be used with TLS connections. |
| retained           | true     | If retained is ``true``,The broker stores the last retained message and the corresponding QoS for that topic.The default value is ``false``.

Below is sample configuration for connecting to Azure IoT Hub by using SAS authentication.
```json
    {
      "mqtt": {
        "server": "ssl://xyz.azure-devices.net:8883",
        "topic": "devices/demo_001/messages/events/",
        "protocolVersion": "3.1.1",
        "qos": 1,
        "clientId": "demo_001",
        "username": "xyz.azure-devices.net/demo_001/?api-version=2018-06-30",
        "password": "SharedAccessSignature sr=*******************",
        "retained": false
      }
    }
```

Below is another sample configuration for connecting to AWS IoT by using certification and privte key auth.

```json
    {
      "mqtt": {
        "server": "ssl://xyz-ats.iot.us-east-1.amazonaws.com:8883",
        "topic": "devices/result",
        "qos": 1,
        "clientId": "demo_001",
        "certificationPath": "keys/d3807d9fa5-certificate.pem",
        "privateKeyPath": "keys/d3807d9fa5-private.pem.key", 
        "insecureSkipVerify": false,
        "retained": false
      }
    }
```

