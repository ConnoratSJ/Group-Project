#!pip install snowflake
import pandas as pd
from google.colab import userdata
from sklearn.linear_model import LinearRegression
from sklearn.neighbors import KNeighborsRegressor
import snowflake.connector
from datetime import datetime
# Snowflake connection details
def get_snowflake_connection():
    return snowflake.connector.connect(
        user = userdata.get('snowflake_username'),
        password = userdata.get('snowflake_password'),
        account = userdata.get('snowflake_account'),
        warehouse = userdata.get('warehouse'),
        database = userdata.get('database'),
        schema = userdata.get('schema'),
    )

# Step 1: Fetch data from Snowflake
def fetch_data_from_snowflake():
    conn = get_snowflake_connection()
    cursor = conn.cursor()


    query = """
        SELECT DATE, LAT, LONG, CURRENT_SPEED, FREE_SPEED
        FROM TRAFFIC_DATA;
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    # Convert to DataFrame

    df = pd.DataFrame(rows, columns=["DATE", "LAT", "LONG", "CURRENT_SPEED", "FREE_SPEED"])
    df["MINUTE"] = df["DATE"].dt.minute
    df["HOUR"] = df["DATE"].dt.hour


    cursor.close()
    conn.close()
    return df



# Step 2: Train KNN model, make predictions, and determine congestion
def train(df, k = 50):
    if df.empty:
        print("No data available for the selected time range. Skipping predictions.")
        return None

    # Feature engineering
    df["normalized_speed"] = df["FREE_SPEED"] / df["FREE_SPEED"].min()
    X = df[["HOUR", "MINUTE", "LAT", "LONG", "FREE_SPEED"]]
    y = df["CURRENT_SPEED"]

    # Train model
    model = KNeighborsRegressor(n_neighbors = k)
    model.fit(X, y)

    return model


def predict(model, year, month, day, hour, minute):
    conn = get_snowflake_connection()
    cursor = conn.cursor()


    query = """
        SELECT LAT, LONG, FREE_SPEED
        FROM PROJECT_DB.ANALYTICS.PREDICTION_FORMAT;
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    # Convert to DataFrame


    prediction_format = pd.DataFrame(rows, columns=["LAT", "LONG", "FREE_SPEED"])
    prediction_format["MINUTE"] = minute
    prediction_format["HOUR"] = hour
    prediction_format =  prediction_format[["HOUR", "MINUTE", "LAT", "LONG", "FREE_SPEED"]]

    prediction_format["PREDICTED_SPEED"] = model.predict(prediction_format)

    prediction_format["IS_CONGESTED"] = prediction_format["PREDICTED_SPEED"] < 0.7 * prediction_format["FREE_SPEED"]

    prediction_format["SPEED_RATIO"] = prediction_format["PREDICTED_SPEED"] / prediction_format["FREE_SPEED"]

    prediction_format["DATE"] = datetime.fromisoformat(year+"-"+month+"-"+day+" "+hour+":"+minute+":00")

    prediction_format.drop(columns = ["MINUTE"])
    prediction_format.drop(columns = ["HOUR"])
    return prediction_format

# Step 3: Insert predictions back into Snowflake
def insert_predictions_to_snowflake(df):
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE OR REPLACE TABLE PROJECT_DB.ANALYTICS.PREDICTIONS (
            DATE TIMESTAMP_NTZ,
            LAT FLOAT,
            LONG FLOAT,
            FREE_SPEED FLOAT,
            PREDICTED_SPEED FLOAT,
            IS_CONGESTED BOOLEAN
        );
    """)



    '''    insert_query = """
        INSERT INTO PROJECT_DB.ANALYTICS.PREDICTIONS (DATE, LAT, LONG, FREE_SPEED, PREDICTED_SPEED, IS_CONGESTED)
        VALUES (TO_TIMESTAMP_NTZ(%s), %s, %s, %s, %s, %s);
    """'''
    for index, row in df.iterrows():
        insert_sql = f"INSERT INTO PROJECT_DB.ANALYTICS.PREDICTIONS(DATE, LAT, LONG, FREE_SPEED, PREDICTED_SPEED, IS_CONGESTED) VALUES(TO_TIMESTAMP_NTZ('{row['DATE']}'), {row['LAT']}, {row['LONG']}, {row['FREE_SPEED']}, {row['PREDICTED_SPEED']}, {row['IS_CONGESTED']});"
        cursor.execute(insert_sql)
    conn.commit()
    cursor.close()
    conn.close()

# Main workflow

print("Fetching data from Snowflake...")
data = fetch_data_from_snowflake()


model = train(data, 50)

prediction = predict(model, '2024', '12', '06', '06', '00')



if prediction is not None:
  print("Uploading predictions to Snowflake...")
  insert_predictions_to_snowflake(prediction)
  print("Predictions uploaded successfully.")