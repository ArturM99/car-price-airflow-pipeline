# Car Price Prediction — Airflow Pipeline

Проект по предсказанию ценовой категории автомобиля, оркестрованный через **Apache Airflow**: модель переобучается по расписанию, а свежие предсказания формируются отдельным шагом DAG.

В отличие от [car-price-prediction](https://github.com/ArturM99/car-price-prediction) (тот же датасет, но с деплоем через FastAPI + Docker + мониторинг Grafana), здесь акцент сделан на **автоматизации ML-пайплайна**: переобучение и инференс запускаются по расписанию без участия человека.

![DAG graph](Graph.png)
![DAG run history](Grid.png)

## Структура проекта

```
.
├── dags/
│   └── car_price_dag.py        # DAG: pipeline → predict, запуск ежедневно в 15:00
├── modules/
│   ├── pipeline.py             # обучение и выбор лучшей модели
│   └── predict.py              # инференс на новых данных последней обученной моделью
├── data/
│   ├── train/
│   │   └── car_listings.csv    # обучающие данные (не входят в репозиторий)
│   ├── test/
│   │   └── *.json               # примеры новых объявлений для предсказания (включены)
│   ├── models/
│   │   └── cars_pipe_*.pkl      # обученные модели с меткой времени в имени (не включены)
│   └── predictions/
│       └── pred_*.csv           # результаты предсказаний (не включены)
└── docker-compose.yaml         # стек Airflow: webserver, scheduler, Postgres, Redis
```

## Как это работает

### DAG (`dags/car_price_dag.py`)

Airflow DAG с id `car_price_prediction`, расписание — ежедневно в 15:00 (`schedule="00 15 * * *"`). Состоит из двух последовательных задач:

```
pipeline_task → predict_task
```

- **`pipeline`** — переобучает модель на актуальных данных.
- **`predict`** — берёт последнюю обученную модель и делает предсказания по новым объявлениям.

Внутри контейнера DAG задаёт `PROJECT_PATH=/opt/airflow/project` и добавляет этот путь в `PYTHONPATH`, поэтому `modules/pipeline.py` и `modules/predict.py` корректно импортируются и находят данные независимо от того, куда установлен сам Airflow.

### Обучение (`modules/pipeline.py`)

1. Читает `data/train/car_listings.csv`.
2. Готовит признаки: убирает неинформативные колонки (`id`, `url`, `region`, `image_url`, `description` и т.д.), обрезает выбросы по году выпуска методом межквартильного размаха, добавляет признаки `short_model` (первое слово модели авто) и `age_category` (новая/средняя/старая).
3. Перебирает три модели — `LogisticRegression`, `RandomForestClassifier`, `SVC` — через 4-фолдовую кросс-валидацию и выбирает лучшую по accuracy.
4. Сохраняет обученный пайплайн в `data/models/cars_pipe_<таймстемп>.pkl` — имя файла содержит дату и время обучения, поэтому история моделей не перезаписывается.

### Инференс (`modules/predict.py`)

1. Находит **самую свежую** модель в `data/models/` (сортировка по имени файла — таймстемп в имени гарантирует правильный порядок).
2. Прогоняет через неё все JSON-файлы из `data/test/` (каждый — одно объявление о продаже).
3. Сохраняет результат в `data/predictions/pred_<таймстемп модели>.csv` с колонками `Car_ID`, `Prediction`.

## Запуск через Docker Compose

Проект работает поверх официального Docker Compose-стенда Apache Airflow (`CeleryExecutor` + PostgreSQL + Redis), с добавленными volume'ами `modules/` и `data/` — поверх стандартных `dags/`, `logs/`, `config/`, `plugins/`.

### 1. Требования

- Установленные Docker и Docker Compose
- Файл `.env` рядом с `docker-compose.yaml` (не входит в репозиторий — см. ниже), минимум с такими переменными:
  ```
  AIRFLOW_UID=50000
  FERNET_KEY=
  ```
  На Linux укажи в `AIRFLOW_UID` свой реальный user id (`id -u`); на Windows/Mac достаточно `50000`.

### 2. Инициализация и запуск

```bash
docker compose up airflow-init
docker compose up -d
```

Это поднимет webserver, scheduler, Postgres и Redis. Первый запуск может занять несколько минут — образ доустанавливает зависимости.

### 3. Открыть интерфейс

`http://localhost:8080` — логин/пароль по умолчанию `airflow` / `airflow`, если не менял в `docker-compose.yaml`.

### 4. Включить и запустить DAG

Найди `car_price_prediction` в списке DAG'ов, включи тумблером (по умолчанию новые DAG выключены), и либо дождись ежедневного запуска в 15:00, либо запусти вручную кнопкой **Trigger DAG**. Вкладка **Graph** показывает структуру `pipeline → predict`, вкладка **Grid** — историю запусков.

## Локальный запуск (без Airflow)

Код обучения/инференса можно запускать и напрямую, без Docker:

```bash
pip install -r requirements.txt
PROJECT_PATH=. python modules/pipeline.py    # обучит модель
PROJECT_PATH=. python modules/predict.py     # сделает предсказания
```

Минимальный набор зависимостей:

```
pandas
scikit-learn
dill
```

## Важно

- `path = os.environ.get('PROJECT_PATH', '.')` — все пути к данным строятся относительно переменной окружения `PROJECT_PATH` (которую задаёт `car_price_dag.py` как `/opt/airflow/project` при запуске в Airflow) или относительно текущей директории при локальном запуске.
- Модель выбирается автоматически из трёх кандидатов по кросс-валидации — какая именно победила, видно в логах задачи `pipeline` в Airflow или в консоли при локальном запуске.
- Целевая переменная — категория цены (классификация), а не точная стоимость автомобиля.
- `data/train/`, `data/models/` и `data/predictions/` исключены из репозитория через `.gitignore` (данные и генерируемые артефакты); примеры объявлений в `data/test/` оставлены как готовые тестовые файлы.

---
🇬🇧 [English version](https://github.com/ArturM99/car-price-airflow-pipeline)
