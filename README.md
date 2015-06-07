#Cloudera Cycle Hire

Sample Cloudera project to investigate the usage characteristics of the Transport for London Cycle Hire Scheme:

http://www.tfl.gov.uk/modes/cycling/barclays-cycle-hire

Note that this project ships with the [cloudera-framework](https://github.com/ggear/cloudera-framework) binary dependencies.

##Compile

```bash
mvn clean install -PDEV,CMP
```

##Build

```bash
mvn clean install -PDEV,BLD
```

##Package

```bash
mvn clean install -PREL,ALL
```

##Release

```bash
mvn release:prepare -B -DreleaseVersion=Major.Minor.Patch -DdevelopmentVersion=Major.Minor.Patch-SNAPSHOT
mvn release:clean
```
