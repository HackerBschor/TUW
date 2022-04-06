# Compile

```bash
cd ChiSquareCalculator/src
hadoop com.sun.tools.javac.Main ChiSquareCalculator.java
jar cf ../wc.jar ChiSquareCalculator*.class
cd ..
hadoop jar wc.jar ChiSquareCalculator /user/e12132344/reviews_devset.json /user/e12132344/ChiSquareResults

hadoop fs -getmerge /user/e12132344/ChiSquareResults/ ChiSquareResultsMerged.txt
```