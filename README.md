# XML-Flattener

![Java 8 / 11 Build on Windows](https://github.com/DevWorxCo/xml-flattener/workflows/Java%208%20/%2011%20Build%20on%20Windows/badge.svg)
![Java 8 / 11 Build on Linux](https://github.com/DevWorxCo/xml-flattener/workflows/Java%208%20/%2011%20Build%20on%20Linux/badge.svg)

The XML Flattener converts hierarchical XML documents into table shaped rectangular data sets consisting of rows and columns. 

## TL;DR - Running the Hello World Example

Assuming you have a version of Java 8+ installed (please see [Adopt OpenJDK](https://adoptopenjdk.net/)), you can download the binary and flatten out the [Hello World Input XML](examples/Hello-World/xml/input-01.xml) to a CSV file with:

### Linux
```

git clone https://github.com/DevWorxCo/xml-flattener.git

cd xml-flattener

wget -P target https://www.devworx.co.uk/assets/jars/xml-flattener-exec.jar

java -jar target/xml-flattener-exec.jar examples/Hello-World/hello-world.yml
```
### Windows
```
git clone https://github.com/DevWorxCo/xml-flattener.git

cd xml-flattener

curl -o target/xml-flattener-exec.jar --create-dirs https://www.devworx.co.uk/assets/jars/xml-flattener-exec.jar

java -jar target/xml-flattener-exec.jar examples/Hello-World/hello-world.yml
```

The above commands will produce the `examples/Hello-World/output/continents-flattened.csv` file. 

![Alt text](flattened-csv-hello-world.png?raw=true "Output CSV Example")

## Introduction

The XML Flattener is a tool that can assist with the following common data use-cases:

* Data extraction over directories containing large numbers of XML files to a form which can then be further analysed by tools such as R Studio, Python Pandas or Excel.
* Transforming deeply nested XML structures to a shape that is consumable by libraries optimised for rectangular data sets (e.g. machine learning libraries or even just end-user spreadsheets).
* Data loading use-cases where the XML data needs to be flattened such that it can be consumed by relational databases and queried via SQL.

If you wish to build this project locally (rather than using the pre-compiled binary) please consult the [README-Building.md](README-Building.md) guide.

## Hello World Example

The ["Hello-World"](examples/Hello-World/README.md) example offers an overview of the functionality that this tool provides. 

Consider the [Input XML](examples/Hello-World/xml/input-01.xml) which lists a number of features per continent. It has three levels of nesting and the `continent` tag has a variable number of attributes.
 
```xml
<?xml version='1.0' encoding='utf-8'?>
<root>
    <title>Hello World</title>
    <source>https://en.wikipedia.org/wiki/Continent</source>
    <description>Simple Example of Features</description>
    <continents>
        <continent area="30370000" population="1287920000" most-populous-city="Lagos, Nigeria">
            <name>Africa</name>
        </continent>
        <continent area="14000000" population="4490" most-populous-city="McMurdo Station" countries="0">
            <name>Antarctica</name>
        </continent>
        <continent area="44579000" population="4545133000" most-populous-city="Shanghai, China">
            <name>Asia</name>
        </continent>
        <continent area="10180000" population="742648000" most-populous-city="Moscow, Russia" demonym="European">
            <name>Europe</name>
        </continent>
        <continent area="24709000" population="587615000" most-populous-city="Mexico City, Mexico">
            <name>North America</name>
        </continent>
        <continent area="8600000" population="41261000" most-populous-city="Sydney, Australia">
            <name>Australia</name>
        </continent>
        <continent area="17840000" population="428240000" most-populous-city="São Paulo, Brazil">
            <name>South America</name>
        </continent>
    </continents>
</root>
```

In order to perform analysis on this XML it needs to be flattened to the following CSV file:

![Alt text](flattened-csv-hello-world.png?raw=true "Output CSV Example")

This can be accomplished with the following YAML flattening specification

```.yaml
name: Hello World Example
inputPath: xml
outputTables:
  - name: continents-flattened
    outputFile: output/continents-flattened.csv
    definition:
      - columnName: Title
        sourceType: xpath
        sourceDef: root/title
      - columnName: Source
        sourceType: xpath
        sourceDef: root/source
      - columnName: Description
        sourceType: xpath
        sourceDef: root/description
      - columnName: Continents
        sourceType: xpath
        sourceDef: root/continents/continent
        explode: true # Create a row for each element
        repeatingList:
          - columnName: Continent-Attrb-
            sourceType: dynAttribute
            sourceDef: "."
            attributeFilter: ".*"
          - columnName: Continent-Name
            sourceType: xpath
            sourceDef: name/text()
```

## Summary of Examples

* *[Hello-World](examples/Hello-World/README.md)* - a showcase of the basic feature set available in this tool. Including standard XPath based extraction, multiple rows for repeating lists and dynamic column generation for variable element attributes.

* *[ODA-International-Subscriptions](examples/ODA-International-Subscriptions/README.md)* -
a real-world example data set from the UK Government (https://data.gov.uk/dataset/e3885716-5e9b-4e29-8dd3-b1c649fb91ed/overseas-development-assistance-oda-international-subscriptions) as demonstrating the use of nested repeating lists and how it is possible to create multiple flattening specifications for the same input XMLs.
 
 
## Building a Native Executable with GraalVM

Execute: 

```
/home/js/dev/java/graalvm-jdk-21.0.1+12.1/bin/native-image -jar target/xml-flattener-exec.jar --trace-class-initialization=com.fasterxml.jackson.databind.ObjectMapper -H:ConfigurationResourceRoots=/home/js/git/terahelix.io/basic-flattener/target/native/agent-output/main

/home/js/dev/java/graalvm-jdk-21.0.1+12.1/bin/native-image -jar target/xml-flattener-exec.jar --trace-class-initialization=com.fasterxml.jackson.databind.ObjectMapper -H:ConfigurationResourceRoots=/home/js/git/terahelix.io/basic-flattener/target/native/agent-output/main --no-fallback


```







 
