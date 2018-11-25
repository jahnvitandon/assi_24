

#Problem
#1. Perform the below given activities:
#a. Take a sample data set of your choice
#b. Apply random forest, logistic regression using Spark R
#c. Predict for new dataset


#Answers
#Environment variables. It is mandatory SPARK_HOME and JAVA_HOME being set
Sys.setenv(SPARK_HOME="/usr/local/lib/R/site-library/spark-1.5/")

.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))

Sys.setenv(JAVA_HOME="/usr/lib/jvm/java-8-oracle/")

#Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-csv_2.10:1.0.3" "sparkr-shell"')

library(SparkR)

#Just stop some running SparkContext
#sparkR.stop()

#Init SparkContext and SQLContext
sc <- sparkR.init(master = "local")
sqlContext <- sparkRSQL.init(sc)
mtcarsDF <- createDataFrame(sqlContext, mtcars)
head(mtcarsDF)
#binomial glm for classification problem
model <- glm(vs ~ mpg + disp + hp + wt , data = mtcarsDF, family = "binomial")

#Warning: suppose to print model coefficients, but its not implemented: https://issues.apache.org/jira/browse/SPARK-9492
#summary(model)

#prediction over same dataset. Just for fun :)
predictions <- predict(model, newData = mtcarsDF )

#select just vs real value and predicted
modelPrediction <- select(predictions, "vs", "prediction")
head(modelPrediction)

#error variable: when vs and predicted differs
modelPrediction$error <- abs(modelPrediction$vs - modelPrediction$prediction)

#modelPrediction is now visible to SQLContext
registerTempTable(modelPrediction, "modelPrediction")

num_errors <- sql(sqlContext, "SELECT count(error) FROM modelPrediction WHERE error = 1")
total_errors <- sql(sqlContext, "SELECT count(error) FROM modelPrediction")

#model error rate
training_acc <- collect(num_errors) / collect(total_errors)
training_acc
