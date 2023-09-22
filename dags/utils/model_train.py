from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, MinMaxScaler
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from xgboost import XGBRegressor
import pandas as pd
import joblib

def train_model(ds):
    df = pd.read_csv('data_output/cleaned.csv')
    cat_cols = ['property_type','property_subtype','kitchen','building_state','region','province']
    use_cols = ['number_rooms', 'living_area', 'surface_land', 'number_facades']

    X = df.drop(columns=['price'], axis=1)
    y = df['price']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=22)

    transformer = ColumnTransformer(
    transformers=[
    ('onehotencoder', OneHotEncoder(handle_unknown='ignore'), cat_cols),
    ('minmaxscaler', MinMaxScaler(), use_cols)
    ])

    grid_results = {'colsample_bytree': 0.3, 'gamma': 0.0, 'learning_rate': 0.15, 'max_depth': 8, 'min_child_weight': 1}

    pipeline = Pipeline([
    ('preprocessor', transformer),
    ('regressor', XGBRegressor(**grid_results))
    ])

    model = pipeline.fit(X_train, y_train)

    joblib.dump(model, f'data_output/history/xgb_model_{ds}.joblib')