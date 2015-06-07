#Cloudera Cycle Hire

Cloudera project that investigates the usage characteristics of the Transport for London Cycle Hire Scheme:

https://www.tfl.gov.uk/modes/cycling/santander-cycles

Note that this project ships with the [cloudera-framework](https://github.com/ggear/cloudera-framework) binary dependencies.

##Requirements

To compile, build and package from source, this project requires:

* JDK 1.7
* Maven 3

##Compile

The project can be compiled for development purposes as per:

```bash
mvn clean install -PDEV,CMP
```

##Build

The project can be built for development purposes, including running unit tests as per:

```bash
mvn clean install -PDEV,BLD
```

##Package

The project can be packaged for release purposes (with assemblies built and exploded in the cloudera-cyclehire-main-assembly and cloudera-cyclehire-main-systest modules build dirs respectively) as per:

```bash
mvn clean install -PREL,ALL
ls cloudera-cyclehire-main/cloudera-cyclehire-main-systest/target/test-assembly
```

##Release

To perform a release:

```bash
export VERSION_RELEASE=0.0.3
export VERSION_HEAD=0.0.4
mvn release:prepare -B -DreleaseVersion=$VERSION_RELEASE -DdevelopmentVersion=$VERSION_HEAD-SNAPSHOT
mvn release:clean
git tag
```
