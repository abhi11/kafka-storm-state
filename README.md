kafka-storm-state
=================

The project uses kafka as the data source. And uses TransactionalTopologies.
Will be later used for storing states by the bolts.
To make storm talk with kafka following artifacts(jars) should be downloaded 
from maven repo(See the pom.xml) :
*     scala-library
*     kafka_2.9.2
*     javax.inject
*     zkclient
*     metrics-core
*     spring-context

These jars should be placed inside the lib directory of storm. The exact versions
 of the above dependencies are specified in [pom.xml](https://github.com/abhi11/kafka-storm-state/blob/master/pom.xml) file.

### Note ###
Check the repo [storm-redis](https://github.com/pict2014/storm-redis) for the complete implementation. 