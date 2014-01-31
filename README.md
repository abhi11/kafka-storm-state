kafka-storm-state
=================

The project uses kafka as the data source. And uses TransactionalTopologies.
Will be later used for storing states by the bolts.
To make storm talk with kafka following artifacts(jars) should be downloaded 
from maven repo :
*     scala-library
*     kafka_2.9.2
*     javax.inject
*     zkclient
*     metrics-core
*     spring-context

These jars should be placed inside the lib directory of storm. The exact versions
 of the above dependencies are specified in pom.xml file.

### To be done ###
* Writing the kafka producer so partitions have equal no. of messages. Changing [kafka-starter](https://github.com/abhi11/kafka-starter)
* Testing on a cluster with multiple machines.
* Writing stateful bolts from (https://github.com/aniketalhat/StatefulBolts)