= ODA International Subscriptions

This XML-Flattener example uses a data set from the UK Government (https://data.gov.uk/dataset/e3885716-5e9b-4e29-8dd3-b1c649fb91ed/overseas-development-assistance-oda-international-subscriptions) as an example of how to turn nested XML into a flattened CSV view.

The YAML file describing the flattening steps can be found here:  [oda-flattening.yml](oda-flattening.yml), while the XML inputs are available in [xml](xml)
 
== Running the Example

```
java -jar ../../target/xml-flattener-exec.jar ./oda-flattening-budgets.yml

java -jar ../../target/xml-flattener-exec.jar ./oda-flattening-transactions.yml

```

