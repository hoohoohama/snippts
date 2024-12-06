import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

# Set or create an experiment
mlflow.set_experiment("Enhanced_Experiment")

# Load data
X, y = load_iris(return_X_y=True)
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=42, stratify=y)

# Parameter grid with more parameters
param_grid = [
    {"n_estimators": 5, "max_depth": 3, "criterion": "gini", "bootstrap": True},
    {"n_estimators": 10, "max_depth": 3, "criterion": "entropy", "bootstrap": True},
    {"n_estimators": 10, "max_depth": 5, "criterion": "gini", "bootstrap": False},
    {"n_estimators": 20, "max_depth": 5, "criterion": "entropy", "bootstrap": False},
    {"n_estimators": 20, "max_depth": 10, "criterion": "gini", "bootstrap": True},
]

for i, params in enumerate(param_grid):
    with mlflow.start_run(run_name=f"run_{i}"):
        # Log parameters
        mlflow.log_param("n_estimators", params["n_estimators"])
        mlflow.log_param("max_depth", params["max_depth"])
        mlflow.log_param("criterion", params["criterion"])
        mlflow.log_param("bootstrap", params["bootstrap"])

        # Train model
        clf = RandomForestClassifier(
            n_estimators=params["n_estimators"],
            max_depth=params["max_depth"],
            criterion=params["criterion"],
            bootstrap=params["bootstrap"],
            random_state=42
        )
        clf.fit(X_train, y_train)
        
        # Predictions and metrics
        preds = clf.predict(X_test)
        accuracy = accuracy_score(y_test, preds)
        precision = precision_score(y_test, preds, average='weighted')
        recall = recall_score(y_test, preds, average='weighted')
        f1 = f1_score(y_test, preds, average='weighted')

        # Log metrics
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("f1_score", f1)

        # Log model artifact
        mlflow.sklearn.log_model(clf, "model")
        
# Retrieve the experiment ID
experiment = mlflow.get_experiment_by_name("Enhanced_Experiment")
experiment_id = experiment.experiment_id

# Example: Search for runs with 'criterion' = 'gini' and 'f1_score' > 0.95
runs_df = mlflow.search_runs(
    experiment_ids=[experiment_id],
    filter_string='params.criterion = "gini" and metrics.f1_score > 0.95',
    order_by=["metrics.f1_score DESC"]
)
print("High f1-score runs using Gini criterion:")
print(runs_df[["run_id", "params.n_estimators", "params.max_depth", "params.criterion", "metrics.f1_score"]])

# Another Query: Runs with bootstrap=False and accuracy > 0.9
runs_df = mlflow.search_runs(
    [experiment_id],
    filter_string='params.bootstrap = "False" and metrics.accuracy > 0.9',
    order_by=["metrics.accuracy DESC"]
)
print("\nHigh accuracy runs without bootstrap:")
print(runs_df[["run_id", "params.n_estimators", "params.max_depth", "params.bootstrap", "metrics.accuracy"]])

# Get the single best run overall by f1_score
top_run = mlflow.search_runs(
    [experiment_id], 
    order_by=["metrics.f1_score DESC"], 
    max_results=1
)
print("\nOverall best run by f1_score:")
print(top_run[["run_id", "metrics.f1_score", "params.n_estimators", "params.max_depth", "params.criterion"]])

