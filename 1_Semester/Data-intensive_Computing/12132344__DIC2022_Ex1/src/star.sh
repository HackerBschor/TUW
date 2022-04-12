STOPWORDS="/user/e12132344/stopwords.txt"
INPUT="/user/pknees/amazon-reviews/full/reviewscombined.json"
OUTPUT="/user/e12132344/ChiSquareResults"

hadoop com.sun.tools.javac.Main ChiSquareCalculator.java
jar cf csc.jar ChiSquareCalculator*.class
cd ..

# full:
hadoop jar src/csc.jar ChiSquareCalculator $STOPWORDS $INPUT $OUTPUT
hadoop fs -getmerge $OUTPUT output.txt
