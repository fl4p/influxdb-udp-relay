# InfluxDB UDP Relay

Receives UDP messages with points in line protocol and forwards them to InfluxDB HTTP API.

# Installation
* Go to your Home Assistant Add-on store and add this repository: `https://github.com/fl4p/home-assistant-addons`
  [![Open your Home Assistant instance and show the dashboard of a Supervisor add-on.](https://my.home-assistant.io/badges/supervisor_addon.svg)](https://my.home-assistant.io/redirect/supervisor_addon/?addon=2af0a32d_batmon&repository_url=https%3A%2F%2Fgithub.com%2Ffl4p%2Fhome-assistant-addons)
* Install influxdb-udp-relay add-on

Works with InfluxDB v1 or v2.


# Configuration
There is not much to configure. You can set the UDP receiver port in the options.

The add-on reads Home Assistant's `/config/configuration.yaml` for InfluxDB credentials.
Make sure you have an influxdb section similar to:
```
influxdb:
  host: a0d7b954-influxdb
  port: 8086
  database: home_assistant
  username: ha
  password: secret
  max_retries: 3
  default_measurement: state
```

If `host` is not specified it falls back to "homeassistant.local"

You can add additinional remote / cloud servers through the options array `additional_servers`.
```
additional_servers:
  - host: "example.com"
    port: 8086
    ssl: true
    username: ""
    password: ""
    database: ""
```