import streamlit as st
from airflow.hooks.S3_hook import S3Hook
import joblib
import pandas as pd

# Streamlit App Title
st.set_page_config(layout="wide")

st.title(":house_with_garden: Immo Eliza Housing Price Prediction :house_with_garden:")

left_column, right_column = st.columns(2)

property_type_options = ['HOUSE', 'APARTMENT']
property_subtype_options = ['HOUSE','VILLA','APARTMENT','MIXED_USE_BUILDING',
                            'APARTMENT_BLOCK','DUPLEX','FLAT_STUDIO','MANSION',
                            'EXCEPTIONAL_PROPERTY','GROUND_FLOOR','PENTHOUSE','TOWN_HOUSE',
                            'TRIPLEX','SERVICE_FLAT','OTHER_PROPERTY','LOFT',
                            'COUNTRY_COTTAGE','CHALET','BUNGALOW','FARMHOUSE',
                            'MANOR_HOUSE','KOT']
kitchen_options = ['USA_HYPER_EQUIPPED' , 'SEMI_EQUIPPED', 'HYPER_EQUIPPED',
                    'USA_INSTALLED', 'INSTALLED', 'NOT_DEFINED', 'USA_SEMI_EQUIPPED',
                    'NOT_INSTALLED', 'USA_UNINSTALLED']
buildingstate_options = ["NEW" , "GOOD" , "TO RENOVATE" , "JUST RENOVATED" , "TO REBUILD"]
region_options = ['Brussels' , 'Flanders' , 'Wallonie']
province_options = ['Brussels','Antwerp','Walloon Brabant','Flemish Brabant',
                    'East Flanders','Limburg','West Flanders','Liège','Hainaut',
                    'Namur','Luxembourg']

with left_column:
    selected_property_type = st.selectbox("PROPERTY TYPE:", property_type_options)
    selected_property_subtype = st.selectbox("PROPERTY SUBTYPE:", property_subtype_options)
    selected_kitchen = st.selectbox("TYPE OF KITCHEN:", kitchen_options)
    selected_buildingstate = st.selectbox("BUILDING STATE:", buildingstate_options)
    selected_region = st.selectbox("REGION", region_options)
    selected_province = st.selectbox("PROVINCE:", province_options)
    selected_numberrooms = st.number_input("NUMBER OF ROOMS:", min_value=0.0, format="%.1f", step=1.0, help="Enter the number of rooms")
    selected_living_area = st.number_input("LIVING AREA (M2):", min_value=0.0, format="%.1f", step=0.5, help="Enter the square meter living area")
    selected_surface_land = st.number_input("SURFACE OF LAND (M2):", min_value=0.0, format="%.1f", step=0.5, help="Enter the square meter total surface area")
    selected_number_facades = st.number_input("NUMBER FACADES:", min_value=0.0, format="%.1f", step=1.0, help="Enter the number of facades")
    selected_latitude = st.number_input("Latitude:", min_value=0.0, format="%.6f", step=0.000001, help="Enter a positive float value for latitude")
    selected_longitude = st.number_input("Longitude:", min_value=0.0, format="%.6f", step=0.000001, help="Enter a positive float value for longitude")

hook = S3Hook('minios3')
# model = hook.download_file(key='s3://xgbmodels3/xgb_model_20230916.joblib')
list_split = []
for i in hook.list_keys('xgbmodels3'):
    split = i.split('_')[2].split('.joblib')
    list_split.append(split)
newest_joblib = max(list_split[0])

model = hook.download_file(key=f's3://xgbmodels3/xgb_model_{newest_joblib}.joblib')

print(f"MODEL DOWNLOADED: {model}")
# Create a Confirm Button
with right_column:
    st.write(" ")  # Add some empty space above the button for vertical centering
    st.write(" ") 
    if st.button(":euro: Predict Price"):
        data = {
            "property_type": selected_property_type,
            "property_subtype": selected_property_subtype,
            "kitchen": selected_kitchen, 
            "building_state": selected_buildingstate, 
            "region":selected_region, 
            "province":selected_province, 
            "number_rooms":selected_numberrooms, 
            "living_area":selected_living_area, 
            "surface_land":selected_surface_land, 
            "number_facades":selected_number_facades, 
            "latitude":selected_latitude, 
            "longitude":selected_longitude, 
        }

        model = joblib.load(model)
        print("MODEL LOADED")

        df = pd.DataFrame(data, index=[0])
        
        predictions = model.predict(df)
        preds = predictions.tolist()

        st.markdown("<h1 style='text-align: center;'>Price prediction:</h1>", unsafe_allow_html=True)

        formatted_preds = "€ {:,}".format(int(preds[0]))

        st.markdown(f"<h2 style='text-align: center;'>{formatted_preds}</h2>", unsafe_allow_html=True)