# Building the XML-Flattener Project Locally

This guide details how you can build this repository locally in order to run the examples, inspect the code or to contribute back improvements and fixes. 

## Prequisites

You need to install the following in order to be able to build this project:

* **Git** - as you are already a GitHub user, you probably already have this setup. If not, please consult the [GitHub Getting Started Guide](https://help.github.com/en/github/getting-started-with-github/set-up-git). You need to clone the project to a local directory.
* **Java 11** - You can obtain Java 11 from [Adopt OpenJDK](https://adoptopenjdk.net/). Alternatively, an excellent production distribution is also maintained by Amazon - [Corretto 11](https://docs.aws.amazon.com/corretto/latest/corretto-11-ug/what-is-corretto-11.html)  
* **Apache Maven** - Obtain the latest version of Apache Maven from [https://maven.apache.org/](https://maven.apache.org/) 

## Verify your `JAVA_HOME` Environment Variable is set

It is important your environment is configured to have the `JAVA_HOME` environment variable set up to point to your installation of the JDK. You may experience the compilation problems described in this article : https://www.baeldung.com/maven-java-home-jdk-jre should this not be the case.

For instructions on how to configure your `JAVA_HOME` environment variable, please refer to the following resources:

* AdoptJDK Installation Guide : https://adoptopenjdk.net/installation.html
* Amazon Corretto Installation Guide : https://docs.aws.amazon.com/corretto/latest/corretto-11-ug/what-is-corretto-11.html 
 
## Building the Project

You are now ready to build the project. You can do a full build by executing the following command :

```
mvn clean install
```

Or if you prefer, you can do so without executing the tests:

```
mvn clean install -DskipTests=true
```
