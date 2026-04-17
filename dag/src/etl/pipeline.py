from pyspark.sql import SparkSession

class AnimeETL:
    def __init__(self, app_name="AnimeETL"):
        # Configure Spark to use Delta Lake
        self.spark = SparkSession.builder.appName(app_name) \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.5.4") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

    def ingest_bronze(self, input_path: str, bronze_path: str):
        raw_df = self.spark.read.csv(input_path, header=True, inferSchema=True, sep='\t')
        
        db_url = "jdbc:postgresql://postgres:5432/airflow"
        properties = {
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver"
        }

        try:
            new_ratings_df = self.spark.read.jdbc(
                url=db_url, 
                table='(SELECT "User_ID", "Anime_ID", "Feedback" FROM user_ratings) as ur', 
                properties=properties
            )
            combined_df = raw_df.unionByName(new_ratings_df)
            print("Successfully loaded new ratings from DB.")
        except Exception as e:
            # If table doesn't exist yet (no new ratings), fallback to just raw_df
            print(f"No new ratings in DB or table missing. Using file data only. Reason: {e}")
            combined_df = raw_df
        
        combined_df.write.format("delta").mode("overwrite").save(bronze_path)

    def process_silver(self, bronze_path: str, silver_path: str):
        bronze_df = self.spark.read.format("delta").load(bronze_path)
        
        clean_df = bronze_df.dropna(subset=["User_ID", "Anime_ID", "Feedback"])
        
        clean_df.write.format("delta").mode("overwrite").save(silver_path)