import requests
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict

app = FastAPI()


class TaskTrigger(BaseModel):
    dag_id: str
    conf: Dict[str, str] = {}


@app.post("/trigger-dag/")
async def trigger_dag(task: TaskTrigger):
    airflow_url = "http://localhost:8080/api/v1/dags/{dag_id}/dagRuns".format(dag_id=task.dag_id)
    headers = {"Content-Type": "application/json"}
    data = {"conf": task.conf}

    try:
        response = requests.post(airflow_url, json=data, auth=('airflow', 'airflow'), headers=headers)
        response.raise_for_status()
        return {"status": "Success", "message": response.json()}
    except Exception as e:
        return {"status": "Failure", "message": str(e)}