# siem-db-connector
Reading SQL database(Currently just MsSql) data and forwarding it as an event on UDP socket.
This app is able to run in two different mode(Master&Slave).
Master node will try to query the database according to config.toml specification.
Another agent can be run on different node in slave mode. slave node at first try to find it's master and sync it's config by master.
so any change in config should just be done on the master node.
If for any reason master node goes down, slave node will go to master mode and continue the operation until master node come back.
These three files should be created for proper operation:
touch /var/siem-db-connector/config.toml
touch /var/siem-db-connector/log/log
touch /var/siem-db-connector/db_track_chnage.json

Sample config.toml configuration is available in this repo.
This app is able to create logs by complicated query specification in its config.toml.
This kind of config allows the user to merge the result of multiple query and create an event.
All query and network operation of this app is handled by async function so this app is able to run multiple queries and forward their results with minimum resource usage.
