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
└── data/
    ├── train/
    │   └── car_listings.csv    # training data (not included in the repo)
    ├── test/
    │   └── *.json               # new listings to predict on
    ├── models/
    │   └── cars_pipe_*.pkl      # trained models, timestamped in the filename
    └── predictions/
        └── pred_*.csv           # prediction results
```

## How it works

### DAG (`dags/car_price_dag.py`)

An Airflow DAG with id `car_price_prediction`, scheduled daily at 15:00 (`schedule="00 15 * * *"`). It consists of two sequential tasks:

```
pipeline_task → predict_task
```

- **`pipeline`** — retrains the model on the latest data.
- **`predict`** — takes the latest trained model and makes predictions on new listings.

### Training (`modules/pipeline.py`)

1. Reads `data/train/car_listings.csv`.
2. Prepares features: drops uninformative columns (`id`, `url`, `region`, `image_url`, `description`, etc.), clips outliers in the manufacture year using the interquartile range method, adds features `short_model` (first word of the car model) and `age_category` (new/mid/old).
3. Compares three models — `LogisticRegression`, `RandomForestClassifier`, `SVC` — via 4-fold cross-validation and picks the best one by accuracy.
4. Saves the trained pipeline to `data/models/cars_pipe_<timestamp>.pkl` — the filename includes the training date and time, so the model history isn't overwritten.

### Inference (`modules/predict.py`)

1. Finds the **most recent** model in `data/models/` (sorted by filename — the timestamp in the name guarantees correct ordering).
2. Runs all JSON files from `data/test/` through it (each file is one listing).
3. Saves the result to `data/predictions/pred_<model timestamp>.csv` with columns `Car_ID`, `Prediction`.

## Installation and running

The project is designed to run inside Airflow, but the training/inference code can also be run locally.

### Local run (without Airflow)

```bash
pip install -r requirements.txt
python modules/pipeline.py    # trains the model
python modules/predict.py     # generates predictions
```

Minimal dependencies:

```
pandas
scikit-learn
dill
```

### Running via Airflow

1. Install Airflow (`pip install apache-airflow`) and initialize the environment (`airflow db init`, `airflow users create ...`).
2. Copy the project to `~/car_price_airflow_pipeline` — the path expected by `dags/car_price_dag.py` (`os.path.expanduser('~/car_price_airflow_pipeline')`).
3. Point Airflow to this project's `dags/` folder in `airflow.cfg` (`dags_folder`), or symlink `dags/car_price_dag.py` into Airflow's default DAGs folder.
4. Start the scheduler and web server:
   ```bash
   airflow scheduler
   airflow webserver
   ```
5. In the Airflow UI (usually `http://localhost:8080`), find the `car_price_prediction` DAG and enable it — it will then run automatically on schedule, or you can trigger it manually with the "Trigger DAG" button.

## Notes

- `path = os.environ.get('PROJECT_PATH', '.')` — all data paths are built relative to the `PROJECT_PATH` environment variable (set by `car_price_dag.py` when running in Airflow) or relative to the current directory for local runs. So the project should be run either through Airflow (the path is configured automatically) or from the repo root.
- The model is automatically selected from three candidates via cross-validation — the winner can be seen in the Airflow logs (`pipeline` task) or in the console for local runs.
- The target variable is a price category (classification), not the exact car price.
---
🇷🇺 [Читать на русском](https://github.com/ArturM99/car-price-airflow-pipeline/tree/RU)
