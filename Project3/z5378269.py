import csv
from scipy import stats
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn import preprocessing 
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score
from sklearn.ensemble import GradientBoostingRegressor, GradientBoostingClassifier
import sys


train_df = pd.read_csv(sys.argv[1], delimiter='\t')
test_df = pd.read_csv(sys.argv[2], delimiter='\t')

# corr_matrix = train_data.corr()
# print(corr_matrix['revenue'].sort_values(ascending=False))

train_df1=pd.get_dummies(train_df, columns=["ATM_Zone","ATM_Placement","ATM_TYPE","ATM_Location_TYPE","ATM_looks","ATM_Attached_to","Day_Type"])
test_df1=pd.get_dummies(test_df, columns=["ATM_Zone","ATM_Placement","ATM_TYPE","ATM_Location_TYPE","ATM_looks","ATM_Attached_to","Day_Type"])
train_df2=pd.get_dummies(train_df, columns=["ATM_Zone","ATM_Placement","ATM_TYPE","ATM_Location_TYPE","ATM_looks","ATM_Attached_to","Day_Type"])
test_df2=pd.get_dummies(test_df, columns=["ATM_Zone","ATM_Placement","ATM_TYPE","ATM_Location_TYPE","ATM_looks","ATM_Attached_to","Day_Type"])

train_df1 = train_df1.fillna(train_df1.mean())
test_df1 = test_df1.fillna(test_df1.mean())
train_df2 = train_df2.fillna(train_df2.mean())
test_df2 = test_df2.fillna(test_df2.mean())

# Part1 Regression ###########################################################################################
val_x = train_df1.drop(["revenue"], axis=1)
val_y = train_df1["revenue"]
test_x = test_df1.drop(["revenue"], axis=1)
test_y = test_df1["revenue"]

scaler = StandardScaler()
X_train = scaler.fit_transform(val_x)

model = GradientBoostingRegressor(n_estimators = 50, learning_rate = 0.6, max_depth = 8, random_state=0)
model.fit(X_train, val_y)


X_test = scaler.transform(test_x)
Y_test = model.predict(X_test)



Y_test=Y_test.astype(int)
# res = stats.pearsonr(Y_test, test_y)
# print(res)
with open('z5378269.PART1.output.csv', 'w') as file:
    file.write('predicted_revenue\n')
    for val in Y_test:
        file.write(f'{val}\n')
    

# Part 2 Classification ######################################################################################

val_x1 = train_df2.drop(["rating"], axis=1)
val_y1 = train_df2["rating"]
test_x1 = test_df2.drop(["rating"], axis=1)
test_y1 = test_df2["rating"]

scaler = StandardScaler()

X_train1 = scaler.fit_transform(val_x1)


model = GradientBoostingClassifier(n_estimators = 50, learning_rate = 0.6, max_depth = 8, random_state=0)
model.fit(X_train1, val_y1)

X_test1 = scaler.transform(test_x1)
Y_test1 = model.predict(X_test1)

# print(Y_test1)
Y_test1=Y_test1.astype(int)

# acc = accuracy_score(test_y1, Y_test1)
# print(acc)

with open('z5378269.PART2.output.csv', 'w') as file:
    file.write('predicted_revenue\n')
    for val in Y_test1:
        file.write(f'{val}\n')
