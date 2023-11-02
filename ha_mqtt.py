from ha_mqtt_discoverable import Settings
from ha_mqtt_discoverable.sensors import Sensor, SensorInfo


# Configure the required parameters for the MQTT broker
mqtt_settings = Settings.MQTT(host="core-mosquitto")
sensor_info = SensorInfo(state_class="measurement", name="MySensor")
settings = Settings(mqtt=mqtt_settings, entity=sensor_info)

mysensor = Sensor(settings)
mysensor.set_state(1.0)

