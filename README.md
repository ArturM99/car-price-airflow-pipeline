# Car Price Prediction — Airflow Pipeline

A car price category prediction project orchestrated via **Apache Airflow**: the model is retrained on a schedule, and fresh predictions are produced as a separate DAG step.

Unlike [car-price-prediction](https://github.com/ArturM99/car-price-prediction) (same dataset, but deployed via FastAPI + Docker + Grafana monitoring), this project focuses on **automating the ML pipeline**: retraining and inference run on a schedule without human involvement.


## Project structure

```
.
├── dags/
│   └── car_price_dag.py        # DAG: pipeline → predict, runs daily at 15:00
├── modules/
│   ├── pipeline.py             # training and best-model selection
│   └── predict.py              # inference on new data using the latest trained model
├── data/
│   ├── train/
│   │   └── car_listings.csv    # training data (not included in the repo)
│   ├── test/
│   │   └── *.json               # sample new listings to predict on (included)
│   ├── models/
│   │   └── cars_pipe_*.pkl      # trained models, timestamped in the filename (not included)
│   └── predictions/
│       └── pred_*.csv           # prediction results (not included)
└── docker-compose.yaml         # Airflow stack: webserver, scheduler, Postgres, Redis
```

## How it works

### DAG (`dags/car_price_dag.py`)

An Airflow DAG with id `car_price_prediction`, scheduled daily at 15:00 (`schedule="00 15 * * *"`). It consists of two sequential tasks:

```
pipeline_task → predict_task
```

- **`pipeline`** — retrains the model on the latest data.
- **`predict`** — takes the latest trained model and makes predictions on new listings.

Inside the container, the DAG sets `PROJECT_PATH=/opt/airflow/project` and adds it to `PYTHONPATH`, so `modules/pipeline.py` and `modules/predict.py` can be imported and can locate the data regardless of where Airflow itself is installed.

### Training (`modules/pipeline.py`)

1. Reads `data/train/car_listings.csv`.
2. Prepares features: drops uninformative columns (`id`, `url`, `region`, `image_url`, `description`, etc.), clips outliers in the manufacture year using the interquartile range method, adds features `short_model` (first word of the car model) and `age_category` (new/average/old).
3. Compares three models — `LogisticRegression`, `RandomForestClassifier`, `SVC` — via 4-fold cross-validation and picks the best one by accuracy.
4. Saves the trained pipeline to `data/models/cars_pipe_<timestamp>.pkl` — the filename includes the training date and time, so the model history isn't overwritten.

### Inference (`modules/predict.py`)

1. Finds the **most recent** model in `data/models/` (sorted by filename — the timestamp in the name guarantees correct ordering).
2. Runs all JSON files from `data/test/` through it (each file is one listing).
3. Saves the result to `data/predictions/pred_<model timestamp>.csv` with columns `Car_ID`, `Prediction`.

## Running via Docker Compose

The project runs on the official Apache Airflow Docker Compose setup (`CeleryExecutor` + PostgreSQL + Redis), with `modules/` and `data/` mounted into the containers as extra volumes on top of the standard `dags/`, `logs/`, `config/`, `plugins/` mounts.

### 1. Prerequisites

- Docker and Docker Compose installed
- A `.env` file next to `docker-compose.yaml` (not included in the repo — see below) with at least:
  ```
  AIRFLOW_UID=50000
  FERNET_KEY=
  ```
  On Linux, set `AIRFLOW_UID` to your actual user id (`id -u`); on Windows/Mac, `50000` works out of the box.

### 2. Initialize and start

```bash
docker compose up airflow-init
docker compose up -d
```

This starts the webserver, scheduler, Postgres, and Redis. First startup can take a few minutes while the image installs dependencies.

### 3. Open the UI

`http://localhost:8080` — default credentials are `airflow` / `airflow` unless changed via `docker-compose.yaml`.

### 4. Enable and trigger the DAG

Find `car_price_prediction` in the DAG list, toggle it on (DAGs are paused by default), and either wait for the daily 15:00 run or trigger it manually via the **Trigger DAG** button. The **Graph** view shows the `pipeline → predict` structure; the **Grid** view shows the run history.

## Running locally (without Airflow)

The training/inference code can also be run directly, outside of Docker:

```bash
pip install -r requirements.txt
PROJECT_PATH=. python modules/pipeline.py    # trains the model
PROJECT_PATH=. python modules/predict.py     # generates predictions
```

Minimal dependencies:

```
pandas
scikit-learn
dill
```

## Notes

- `path = os.environ.get('PROJECT_PATH', '.')` — all data paths are built relative to the `PROJECT_PATH` environment variable (set to `/opt/airflow/project` by `car_price_dag.py` when running in Airflow) or relative to the current directory for local runs.
- The model is automatically selected from three candidates via cross-validation — the winner can be seen in the Airflow task logs (`pipeline` task) or in the console for local runs.
- The target variable is a price category (classification), not the exact car price.
- `data/train/`, `data/models/`, and `data/predictions/` are excluded from the repo via `.gitignore` (data files and generated artifacts); `data/test/` sample listings are kept as ready-to-use examples.

---
🇷🇺 [Читать на русском](https://github.com/ArturM99/car-price-airflow-pipeline/tree/RU)
