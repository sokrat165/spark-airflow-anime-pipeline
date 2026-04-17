import os
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text

app = FastAPI(title="Anime Recommendations API")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@postgres/airflow")
engine = create_engine(DATABASE_URL)

class Rating(BaseModel):
    user_id: int
    anime_id: int
    feedback: int

@app.on_event("startup")
def startup_event():
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS user_ratings (
                "User_ID" INTEGER,
                "Anime_ID" INTEGER,
                "Feedback" INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/rate")
def rate_anime(rating: Rating):
    try:
        with engine.begin() as connection:
            connection.execute(
                text("""
                    INSERT INTO user_ratings ("User_ID", "Anime_ID", "Feedback") 
                    VALUES (:uid, :aid, :fb)
                """),
                {"uid": rating.user_id, "aid": rating.anime_id, "fb": rating.feedback}
            )
        return {"message": "Rating saved successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/recommend/{user_id}")
def get_recommendations(user_id: int):
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text('SELECT recommendations FROM anime_recommendations WHERE "User_ID" = :uid'), 
                {"uid": user_id}
            ).fetchone()
            
            if result is None:
                raise HTTPException(status_code=404, detail="User not found or no recommendations available.")
            
            # The recommendations are stored as JSON string
            recs_json = result[0]
            recommendations = json.loads(recs_json)
            
            return {
                "user_id": user_id,
                "recommendations": recommendations
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
