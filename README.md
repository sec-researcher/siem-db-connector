# siem-db-connector
Reading SQL database(Currently just MsSql) data and forwarding it as an event on UDP socket. This app is able to run in two different mode(Master&Slave). Master node will try to query the database according to config.toml specification. Another agent can be run on different node in slave mode. slave node at first try to find it's master and sync it's config by master. so any change in config should just be done on the master node.

If for any reason master node goes down, slave node will go to master mode and continue the operation until master node come back. These three files should be created for proper operation:
```
touch /var/siem-db-connector/config.toml
touch /var/siem-db-connector/log/log
touch /var/siem-db-connector/db_track_chnage.json #
```
Sample config.toml configuration is available in this repo. This app is able to create logs by complicated query specification in its config.toml. This kind of config allows the user to merge the result of multiple query in one event. All query and network operation of this app is handled by async function so this app is able to run multiple queries and forward their results with minimum resource usage.

## There are 3 types of configuration object in this app:

### 1.General config
```
listening_addr = "10.235.20.1:8080"
```
Indicate IP address and listening port for the app, The app use this port to communicate with its partner
```
peer_addr= "10.235.20.20:8080"      #IP address and listening port of the partner
app_socket="/tmp/db"                #unix socket path to prevent running multiple instance
ping_duration = 2000                #pause time between agent communication, and config sync
default_role = "master"             #default app role it could be {master, slave}
log_server = "127.0.0.1:10100"      #if the user did not specify log server per log_source the app use this address as default log server
```
### 2.[log_sources]
```
name="test"             #log source name
addr = "192.168.77.129" #mssql server ip address
port= 1433              #mssql server port number
username= "test"        #username with proper access to run query
pass= "123"             #password
query= "select * FROM [test].[dbo].[identity] where id>??" #The query that should run on DB, with notations like id>?? we can aware the app for using this field as counter
counter_field = "id"     #Counter field name
counter_default_value="0"    #For first time the app use this value as counter default value, after that the new value will be store in db_track_change.json
log_server="127.0.0.1:10200,127.0.0.1:10201" #Any row readed from DB will be sent on udp to this list, it's possible to define multiple server by comma(,) separation
pause_duration = 2000   #defining pause time between query call
log_mode="Both"         #log mode can have these values {Both,Net,File} Net just sent log over udp, File store recieved log as csv on specified path in path property
path="/tmp/test.csv"    #specify the location of csv file for saving log, this propert only works if log_mode set to File or Both.
set_current_time=1      #add a date field at the begenning of each event by the value of current local time&date
```
### 3.[[comp]]
```
name="comp1"                  #comp stands for complicated, this property define the name of complicated query this name should be unique
result="compq1, cat=compq2,"  #define result format, comp1 will be replace with the result of [[comp.log_sources]].name="comp1" and so on

[[comp.log_sources]]          #this is a log_sources object, full description of properties is available at above(num 2).
name="compq1"
addr = "192.168.77.129"
port= 1433
username= "test"
pass= "123"
query= "SELECT title as log_field_name  FROM [test].[dbo].[cat] where id=1"
set_current_time=1
```
