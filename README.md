# Shopping Microservice

## Run RPC Server
```
cd /src
javac *.java
java RPCServer <ip_of_rabbitmq_server>
```

## Send requests to the message queue
```
cd /src
javac *.java
java RPCClient <ip_of_rabbitmq_server>
```
