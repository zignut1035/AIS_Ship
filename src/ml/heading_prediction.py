import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler, LabelEncoder, MinMaxScaler
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, ExtraTreesRegressor, AdaBoostRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression, LogisticRegression, Ridge
from sklearn.metrics import accuracy_score, mean_squared_error, r2_score, classification_report, confusion_matrix, mean_absolute_error
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats
from xgboost import XGBRegressor

data = pd.read_csv('/home/treenut/ais-data-engineering-project/data/merged_with_sea_ais_data.csv')

# Drop rows with any NaN values
data_clean = data.dropna(how='any')

# Combine all unique values from the three columns
combined_ports = set([
    'FIVAA', 'FIPOR', 'FIANK', 'FIUKI', 'FIRAU', 'FIKOK', # port_to_visit
    'SEHLD', 'DERSK', 'SESOR', 'LVMRX', 'SEHAN', 'FITOR', 'SEHOG', # prev_port
    'SEHLD', 'DERSK', 'FIUKI', 'SENRK', 'FRFEC', 'FRSNR', 'SELAA' # next_port
])

# Create the port mapping dictionary
port_mapping = {port: idx + 1 for idx, port in enumerate(combined_ports)}

data_clean['port_to_visit_numerical'] = data_clean['port_to_visit'].map(port_mapping)
data_clean['prev_port_numerical'] = data_clean['prev_port'].map(port_mapping)
data_clean['next_port_numerical'] = data_clean['next_port'].map(port_mapping)

# Convert port_call_timestamp and vessel_timestamp to datetime objects
data_clean['port_call_timestamp'] = pd.to_datetime(data_clean['port_call_timestamp'])
data_clean['vessel_timestamp'] = pd.to_datetime(data_clean['vessel_timestamp'])

# Convert the datetime objects to Unix timestamps (in seconds)
data_clean['port_call_timestamp_numeric'] = data_clean['port_call_timestamp'].astype(int) / 10**9  # seconds
data_clean['vessel_timestamp_numeric'] = data_clean['vessel_timestamp'].astype(int) / 10**9  # seconds

# Calculate the difference in nanoseconds (no need for conversion to seconds if nanosecond precision is required)
data_clean['time_diff_ns'] = data_clean['vessel_timestamp'].diff().dt.total_seconds() * 1e9  # in nanoseconds

# Fill NaN values (the first row will have NaN)
data_clean['time_diff_ns'].fillna(0, inplace=True)

# Convert True/False to 1/0 for 'pos_acc' and 'raim' columns
data_clean['pos_acc'] = data_clean['pos_acc'].astype(int)
data_clean['raim'] = data_clean['raim'].astype(int)

# Shift the target column by 30 rows for 5 minutes ahead (since data is updated every 10 seconds)
data_clean['target_cog'] = data_clean.groupby('mmsi')['cog'].shift(-30)

# Drop rows where target_cog is NaN (e.g., the last 30 rows in the dataset)
data_clean = data_clean.dropna(subset=['target_cog'])

data_clean['vessel_timestamp'] = pd.to_datetime(data_clean['vessel_timestamp']).astype('int64') // 10**9
data_clean['port_call_timestamp'] = pd.to_datetime(data_clean['port_call_timestamp']).astype('int64') // 10**9

# Convert categorical columns to numerical values
data_clean['site_name'] = data_clean['site_name'].astype('category').cat.codes
data_clean['sea_state'] = data_clean['sea_state'].astype('category').cat.codes
# Calculate the frequency of each unique mmsi value
mmsi_freq = data_clean['mmsi'].value_counts() / len(data_clean)

# Map the frequency of each mmsi value to the 'mmsi_freq' column
data_clean['mmsi_encoded'] = data_clean['mmsi'].map(mmsi_freq)

# Optionally, drop the original 'mmsi' column if you no longer need it
data_clean = data_clean.drop(columns=['mmsi'])

# Set target and features
target_column = 'target_cog'  # The column we want to predict

features_columns = [
    "sog",  # Speed Over Ground
    "rot",  # Rate of Turn
    "heading",  # Current heading
    "vessel_latitude", "vessel_longitude",  # Vessel position
    "wind_wave_dir",  # Wind and wave direction
    "heel_angle",  # Vessel tilt
    "temperature",  # Weather conditions
    "sea_latitude", "sea_longitude",  # Sea position (optional)
    "vessel_timestamp_numeric",  # Time-based feature
    "port_call_timestamp_numeric",  # Port time feature
    "nav_stat", "raim",  # Navigation-related
    "port_to_visit_numerical", "prev_port_numerical", "next_port_numerical",  # Categorical port data
    "time_diff_ns"  # Add this missing feature
]


# Split data into features (X) and target (y)
X = data_clean[features_columns]
y = data_clean[target_column]

# Initialize the scaler
scaler = StandardScaler()

# Scale selected features
X[['sog', 'time_diff_ns', 'heading', 'vessel_latitude', 'vessel_longitude']] = scaler.fit_transform(
    X[['sog', 'time_diff_ns', 'heading', 'vessel_latitude', 'vessel_longitude']]
)
# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize the models
rfr = RandomForestRegressor()
etr = ExtraTreesRegressor()
abr = AdaBoostRegressor()
gbr = GradientBoostingRegressor()
lnr = LinearRegression()
xgb = XGBRegressor()

# List of models
models = [rfr, etr, abr, gbr, lnr, xgb]
names = ["Random Forest", "Extra Trees", "Ada Boost", "Gradient Boosting", "Linear Regression", "XGBoost"]

# Initialize lists to store results
r2s, mses = [], []

# Function to train and evaluate models
def training(model, X_train, y_train, X_test, y_test):
    model.fit(X_train, y_train)
    pred = model.predict(X_test)
    mse = mean_squared_error(y_test, pred)
    r2 = r2_score(y_test, pred)
    return mse, r2

# Train each model and print the results
for i, j in zip(models, names):
    print("*" * 30)
    print(f"Training {j}...")
    mse, r2 = training(i, X_train, y_train, X_test, y_test)
    print(f"MSE: {mse}")
    print(f"R-squared: {r2}\n")
    r2s.append(r2)
    mses.append(mse)

# Compare the models' performance
print("*" * 30)
print("Model Performance Comparison:")
for name, mse, r2 in zip(names, mses, r2s):
    print(f"{name} - MSE: {mse}, R-squared: {r2}")

feature_importances = gbr.feature_importances_
feature_names = X.columns

# Plot feature importances
plt.barh(feature_names, feature_importances)
plt.xlabel("Feature Importance")
plt.title("Feature Importance for Predicting COG")
plt.show()

# Train the Random Forest model (you can use any model, here we use RandomForest)
gbr.fit(X_train, y_train)

# Predict the COG on the test set
y_pred = gbr.predict(X_test)

# Actual vs Predicted COG values
actual_vs_predicted = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred})

# Display the actual vs predicted values
print(actual_vs_predicted)

# Plotting the actual vs predicted values
plt.figure(figsize=(10, 6))
plt.scatter(y_test, y_pred, color='blue', alpha=0.6)
plt.plot([min(y_test), max(y_test)], [min(y_test), max(y_test)], color='red', linewidth=2)  # Ideal line (y = x)
plt.title('Actual vs Predicted COG')
plt.xlabel('Actual COG')
plt.ylabel('Predicted COG')
plt.show()

