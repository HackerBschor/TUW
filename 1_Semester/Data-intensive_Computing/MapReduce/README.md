# Compile

```bash
cd ChiSquareCalculator/src
hadoop com.sun.tools.javac.Main ChiSquareCalculator.java
jar cf ../wc.jar ChiSquareCalculator*.class
cd ..

# full:
hadoop jar wc.jar ChiSquareCalculator /user/e12132344/stopwords.txt /user/pknees/amazon-reviews/full/reviewscombined.json /user/e12132344/ChiSquareResults
# test:
hadoop fs -getmerge /user/e12132344/ChiSquareResults/ ChiSquareResultsMerged.txt
```