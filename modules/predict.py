import dill
import pandas as pd
import json
import os

from glob import glob

path = os.environ.get('PROJECT_PATH', '.')

def predict():
    model_files = sorted(glob(f'{path}/data/models/cars_pipe_*.pkl'))
    model_filename = model_files[-1]
    with open(model_filename, 'rb') as file:
        loaded_model = dill.load(file)
    test_files = glob(f'{path}/data/test/*.json')
    predictions = []
    for test_file in test_files:
        with open(test_file, 'r', encoding='utf-8') as file:
            data = json.load(file)
            df = pd.DataFrame([data])
            prediction = loaded_model.predict(df)
            predictions.append({'Car_ID': data['id'], 'Prediction': prediction[0]})
    result = pd.DataFrame(predictions)
    os.makedirs(f'{path}/data/predictions', exist_ok=True)
    model_name = os.path.splitext(os.path.basename(model_filename))[0].split('_')[2]
    prediction_filename = f'{path}/data/predictions/pred_{model_name}.csv'
    result.to_csv(prediction_filename, index=False)





if __name__ == '__main__':
    predict()
