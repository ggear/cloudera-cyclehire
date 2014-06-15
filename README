#Cloudera Cycle Hire

Sample Cloudera project to investigate the usage characteristics of the Transport for London Cycle Hire Scheme:

http://www.tfl.gov.uk/modes/cycling/barclays-cycle-hire

##Build

```bash
mvn clean install -PDEV,ITR
```

##Package

```bash
mvn clean install -PREL,PKG
```

##Test

```bash
mvn clean install -PREL,ALL
```

##Release

```bash
mvn release:prepare -B -DreleaseVersion=Major.Minor.Patch -DdevelopmentVersion=Major.Minor.Patch-SNAPSHOT
mvn release:clean
```
