import os
import mlflow
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password123"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"

class ModelTrainer:
    def __init__(self, tracking_uri: str, experiment_name: str):
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)

    def train_als(self, data):
        with mlflow.start_run():
            # Split data into train and test
            train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

            # Build ALS model instance
            als = ALS(
                userCol="User_ID", 
                itemCol="Anime_ID", 
                ratingCol="Feedback",
                coldStartStrategy="drop",
                nonnegative=True
            )
            
            # Setup Evaluation
            evaluator = RegressionEvaluator(
                metricName="rmse", 
                labelCol="Feedback", 
                predictionCol="prediction"
            )

            param_grid = ParamGridBuilder()\
                .addGrid(als.rank, [10, 15])\
                .addGrid(als.maxIter, [10])\
                .addGrid(als.regParam, [0.1, 0.05])\
                .build()

            tvs = TrainValidationSplit(
                estimator=als,
                estimatorParamMaps=param_grid,
                evaluator=evaluator,
                trainRatio=0.8
            )

            tvs_model = tvs.fit(train_data)
            best_model = tvs_model.bestModel

            predictions = best_model.transform(test_data)
            rmse = evaluator.evaluate(predictions)

            # Extract best params
            best_rank = best_model._java_obj.parent().getRank()
            best_maxIter = best_model._java_obj.parent().getMaxIter()
            best_regParam = best_model._java_obj.parent().getRegParam()

            mlflow.log_params({
                "best_rank": best_rank,
                "best_maxIter": best_maxIter,
                "best_regParam": best_regParam
            })
            mlflow.log_metric("rmse", rmse)
            
            # Save the best model
            mlflow.spark.log_model(best_model, "als-model", registered_model_name="Anime_ALS_Model")
            
            # Generate top 10 recommendations for all users
            recommendations = best_model.recommendForAllUsers(10)

            return best_model, recommendations