#!/usr/bin/env python3
"""
VegPro ML Pipeline - Simplified for Dataform Integration
=========================================================
This script trains ML models using feature tables created by Dataform.
Feature engineering is handled by Dataform (.sqlx files).

Usage:
    python run_pipeline.py --all        # Train all models
    python run_pipeline.py --demand     # Train demand model only
    python run_pipeline.py --dispatch   # Train dispatch model only
    python run_pipeline.py --production # Train production model only
    python run_pipeline.py --rejection  # Train rejection model only

Prerequisites:
    - Run Dataform to create feature tables first
    - Feature tables: ml_demand_features, ml_dispatch_features, 
                      ml_production_features, ml_rejection_features
"""

import argparse
import logging
from datetime import datetime
import pandas as pd
import numpy as np
from google.cloud import bigquery
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import warnings
warnings.filterwarnings('ignore')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = "adrians-dfpr"
DATASET = "vegpro_gold_v3"

# Model configurations - which features to use for each model
MODEL_CONFIGS = {
    'demand': {
        'table': 'ml_demand_features',
        'target': 'target_next_week',
        'features': [
            'week_sin', 'week_cos', 'is_valentines', 'is_easter_period',
            'is_mothers_day', 'is_christmas', 'is_summer', 'quarter',
            'stems_lag_1w', 'stems_lag_2w', 'stems_lag_4w', 'stems_lag_52w',
            'stems_rolling_4w_avg', 'stems_rolling_8w_avg',
            'variety_avg_stems', 'variety_std_stems', 'variety_week_count',
            'customer_avg_stems', 'customer_std_stems'
        ],
        'group_cols': ['variety_name', 'sub_customer_name'],
        'description': 'Demand (Orders) Forecasting'
    },
    'dispatch': {
        'table': 'ml_dispatch_features',
        'target': 'target_next_week',
        'features': [
            'week_sin', 'week_cos', 'is_valentines', 'is_easter_period',
            'is_mothers_day', 'is_christmas', 'is_summer', 'quarter',
            'dispatch_lag_1w', 'dispatch_lag_2w', 'dispatch_lag_4w', 'dispatch_lag_52w',
            'dispatch_rolling_4w_avg', 'dispatch_rolling_8w_avg',
            'orders_lag_1w', 'orders_lag_4w',
            'current_fill_rate', 'fill_rate_rolling_4w',
            'customer_avg_dispatch', 'customer_std_dispatch', 'customer_avg_fill_rate'
        ],
        'group_cols': ['sub_customer_name'],
        'description': 'Dispatch (Actuals) Forecasting'
    },
    'production': {
        'table': 'ml_production_features',
        'target': 'target_next_week',
        'features': [
            'week_sin', 'week_cos', 'is_growing_season', 'quarter',
            'stems_lag_1w', 'stems_lag_2w', 'stems_lag_4w', 'stems_lag_52w',
            'stems_rolling_4w_avg', 'stems_rolling_8w_avg',
            'farm_avg_stems', 'farm_std_stems',
            'variety_avg_stems', 'variety_std_stems'
        ],
        'group_cols': ['farm', 'variety_name'],
        'description': 'Production (Harvest) Forecasting'
    },
    'rejection': {
        'table': 'ml_rejection_features',
        'target': 'target_rejection_rate',
        'features': [
            'week_sin', 'week_cos', 'is_summer_heat', 'is_winter',
            'is_peak_demand', 'quarter',
            'rejection_rate_lag_1w', 'rejection_rate_lag_2w', 'rejection_rate_lag_4w',
            'rejection_rate_rolling_4w', 'rejection_rate_rolling_8w',
            'farm_avg_rejection_rate', 'variety_avg_rejection_rate'
        ],
        'group_cols': ['farm', 'variety_name'],
        'description': 'Rejection Rate Forecasting'
    }
}


def get_client():
    """Get BigQuery client."""
    return bigquery.Client(project=PROJECT_ID)


def load_feature_table(model_name: str) -> pd.DataFrame:
    """Load feature table from BigQuery."""
    config = MODEL_CONFIGS[model_name]
    table = config['table']
    
    client = get_client()
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{table}`"
    
    logger.info(f"Loading {table}...")
    df = client.query(query).to_dataframe()
    logger.info(f"  Loaded {len(df):,} rows")
    
    return df


def check_feature_table_exists(model_name: str) -> bool:
    """Check if feature table exists in BigQuery."""
    config = MODEL_CONFIGS[model_name]
    table = config['table']
    
    client = get_client()
    try:
        client.get_table(f"{PROJECT_ID}.{DATASET}.{table}")
        return True
    except Exception:
        return False


def prepare_data(df: pd.DataFrame, model_name: str):
    """Prepare features and target for training."""
    config = MODEL_CONFIGS[model_name]
    feature_cols = config['features']
    target_col = config['target']
    
    # Get available features (some might not exist)
    available_features = [f for f in feature_cols if f in df.columns]
    missing_features = [f for f in feature_cols if f not in df.columns]
    
    if missing_features:
        logger.warning(f"  Missing features: {missing_features}")
    
    if target_col not in df.columns:
        raise ValueError(f"Target column '{target_col}' not found in data")
    
    # Filter to rows with valid target and features
    df_clean = df.dropna(subset=[target_col] + available_features)
    
    X = df_clean[available_features].fillna(0)
    y = df_clean[target_col]
    
    # Keep metadata for later analysis
    meta_cols = ['year', 'week_number', 'week_key'] + config['group_cols']
    meta_cols = [c for c in meta_cols if c in df_clean.columns]
    metadata = df_clean[meta_cols].copy()
    
    logger.info(f"  Prepared {len(X):,} samples with {len(available_features)} features")
    
    return X, y, metadata, available_features


def train_expanding_window(df: pd.DataFrame, model_name: str, min_train_weeks: int = 12):
    """
    Train model using expanding window approach.
    For each week N, train on weeks 1 to N-1, predict week N.
    This gives predictions for ALL weeks (not just test set).
    """
    config = MODEL_CONFIGS[model_name]
    feature_cols = config['features']
    target_col = config['target']
    group_cols = config['group_cols']
    
    available_features = [f for f in feature_cols if f in df.columns]
    
    # Get unique weeks sorted
    df['year_week'] = df['year'] * 100 + df['week_number']
    weeks = sorted(df['year_week'].unique())
    
    logger.info(f"  Training expanding window: {len(weeks)} weeks")
    
    all_predictions = []
    
    for i, target_week in enumerate(weeks[min_train_weeks:], start=min_train_weeks):
        # Train on all weeks before target_week
        train_weeks = weeks[:i]
        
        train_df = df[df['year_week'].isin(train_weeks)]
        test_df = df[df['year_week'] == target_week]
        
        if len(test_df) == 0:
            continue
        
        # Prepare training data
        train_clean = train_df.dropna(subset=[target_col] + available_features)
        if len(train_clean) < 100:
            continue
            
        X_train = train_clean[available_features].fillna(0)
        y_train = train_clean[target_col]
        
        # Prepare test data
        test_clean = test_df.dropna(subset=available_features)
        if len(test_clean) == 0:
            continue
            
        X_test = test_clean[available_features].fillna(0)
        
        # Train model (use GradientBoosting for speed)
        model = GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42)
        model.fit(X_train, y_train)
        
        # Predict
        predictions = model.predict(X_test)
        
        # Store predictions with metadata
        pred_df = test_clean[['year', 'week_number', 'week_key'] + group_cols].copy()
        pred_df['predicted'] = predictions
        pred_df['actual'] = test_clean[target_col].values if target_col in test_clean.columns else np.nan
        pred_df['model_type'] = model_name
        pred_df['train_weeks'] = len(train_weeks)
        
        all_predictions.append(pred_df)
        
        if (i - min_train_weeks + 1) % 10 == 0:
            logger.info(f"    Processed {i - min_train_weeks + 1}/{len(weeks) - min_train_weeks} weeks")
    
    if all_predictions:
        result_df = pd.concat(all_predictions, ignore_index=True)
        logger.info(f"  Generated {len(result_df):,} predictions")
        return result_df
    else:
        logger.warning("  No predictions generated")
        return pd.DataFrame()


def train_final_model(df: pd.DataFrame, model_name: str, test_size: float = 0.2):
    """
    Train final production model with train/test split.
    Returns model performance metrics and feature importance.
    """
    config = MODEL_CONFIGS[model_name]
    
    X, y, metadata, feature_names = prepare_data(df, model_name)
    
    # Time-based split
    n_train = int(len(X) * (1 - test_size))
    X_train, X_test = X.iloc[:n_train], X.iloc[n_train:]
    y_train, y_test = y.iloc[:n_train], y.iloc[n_train:]
    meta_test = metadata.iloc[n_train:]
    
    logger.info(f"  Train: {len(X_train):,}, Test: {len(X_test):,}")
    
    # Train multiple models and pick best
    models = {
        'RandomForest': RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1),
        'GradientBoosting': GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42)
    }
    
    # Try XGBoost and LightGBM if available
    try:
        from xgboost import XGBRegressor
        models['XGBoost'] = XGBRegressor(n_estimators=100, max_depth=5, random_state=42, verbosity=0)
    except ImportError:
        pass
    
    try:
        from lightgbm import LGBMRegressor
        models['LightGBM'] = LGBMRegressor(n_estimators=100, max_depth=5, random_state=42, verbose=-1)
    except ImportError:
        pass
    
    best_model_name = None
    best_model = None
    best_wmape = float('inf')
    results = []
    
    for name, model in models.items():
        logger.info(f"    Training {name}...")
        model.fit(X_train, y_train)
        
        y_pred = model.predict(X_test)
        
        # Calculate metrics
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2 = r2_score(y_test, y_pred)
        wmape = np.sum(np.abs(y_test - y_pred)) / np.sum(np.abs(y_test)) * 100
        
        results.append({
            'model_type': model_name,
            'algorithm': name,
            'mae': mae,
            'rmse': rmse,
            'r2': r2,
            'wmape': wmape,
            'train_samples': len(X_train),
            'test_samples': len(X_test)
        })
        
        logger.info(f"      MAE: {mae:,.0f}, RMSE: {rmse:,.0f}, WMAPE: {wmape:.1f}%")
        
        if wmape < best_wmape:
            best_wmape = wmape
            best_model_name = name
            best_model = model
    
    logger.info(f"  Best model: {best_model_name} (WMAPE: {best_wmape:.1f}%)")
    
    # Get feature importance from best model
    if hasattr(best_model, 'feature_importances_'):
        importance_df = pd.DataFrame({
            'model_type': model_name,
            'feature': feature_names,
            'importance': best_model.feature_importances_
        }).sort_values('importance', ascending=False)
    else:
        importance_df = pd.DataFrame()
    
    # Create predictions dataframe
    predictions_df = meta_test.copy()
    predictions_df['actual'] = y_test.values
    predictions_df['predicted'] = best_model.predict(X_test)
    predictions_df['model_type'] = model_name
    predictions_df['algorithm'] = best_model_name
    
    return pd.DataFrame(results), importance_df, predictions_df


def calculate_accuracy_breakdowns(predictions_df: pd.DataFrame, model_name: str):
    """Calculate accuracy by customer, variety, and week."""
    config = MODEL_CONFIGS[model_name]
    group_cols = config['group_cols']
    
    # Calculate errors
    predictions_df = predictions_df.copy()
    predictions_df['error'] = predictions_df['actual'] - predictions_df['predicted']
    predictions_df['abs_error'] = np.abs(predictions_df['error'])
    predictions_df['pct_error'] = np.abs(predictions_df['error']) / predictions_df['actual'].clip(lower=1) * 100
    
    breakdowns = {}
    
    # By week
    weekly = predictions_df.groupby(['year', 'week_number']).agg({
        'actual': 'sum',
        'predicted': 'sum',
        'abs_error': 'sum'
    }).reset_index()
    weekly['wmape'] = weekly['abs_error'] / weekly['actual'].clip(lower=1) * 100
    weekly['model_type'] = model_name
    breakdowns['weekly'] = weekly
    
    # By group columns (customer, variety, farm)
    for col in group_cols:
        if col in predictions_df.columns:
            grp = predictions_df.groupby(col).agg({
                'actual': 'sum',
                'predicted': 'sum',
                'abs_error': 'sum'
            }).reset_index()
            grp['wmape'] = grp['abs_error'] / grp['actual'].clip(lower=1) * 100
            grp['model_type'] = model_name
            breakdowns[col] = grp
    
    return breakdowns


def save_to_bigquery(df: pd.DataFrame, table_name: str, write_disposition: str = 'WRITE_TRUNCATE'):
    """Save dataframe to BigQuery."""
    client = get_client()
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    
    logger.info(f"  Saved {len(df):,} rows to {table_name}")


def run_model_pipeline(model_name: str):
    """Run complete pipeline for a single model."""
    logger.info(f"\n{'='*60}")
    logger.info(f"Running {MODEL_CONFIGS[model_name]['description']} Pipeline")
    logger.info(f"{'='*60}")
    
    # Check if feature table exists
    if not check_feature_table_exists(model_name):
        logger.error(f"Feature table not found: {MODEL_CONFIGS[model_name]['table']}")
        logger.error("Run Dataform first to create feature tables!")
        return None
    
    # Load data
    df = load_feature_table(model_name)
    
    if len(df) == 0:
        logger.error("No data found in feature table")
        return None
    
    # Train with expanding window (for full history predictions)
    logger.info("\n1. Training with expanding window...")
    expanding_predictions = train_expanding_window(df, model_name)
    
    # Train final model
    logger.info("\n2. Training final model...")
    metrics_df, importance_df, final_predictions = train_final_model(df, model_name)
    
    # Calculate accuracy breakdowns
    logger.info("\n3. Calculating accuracy breakdowns...")
    breakdowns = calculate_accuracy_breakdowns(final_predictions, model_name)
    
    return {
        'expanding_predictions': expanding_predictions,
        'final_predictions': final_predictions,
        'metrics': metrics_df,
        'importance': importance_df,
        'breakdowns': breakdowns
    }


def save_all_results(all_results: dict):
    """Save all results to BigQuery."""
    logger.info("\n" + "="*60)
    logger.info("Saving Results to BigQuery")
    logger.info("="*60)
    
    # Combine predictions from all models
    all_predictions = []
    all_metrics = []
    all_importance = []
    all_weekly = []
    all_customer = []
    all_variety = []
    
    for model_name, results in all_results.items():
        if results is None:
            continue
            
        if len(results['expanding_predictions']) > 0:
            all_predictions.append(results['expanding_predictions'])
        if len(results['final_predictions']) > 0:
            all_predictions.append(results['final_predictions'])
        if len(results['metrics']) > 0:
            all_metrics.append(results['metrics'])
        if len(results['importance']) > 0:
            all_importance.append(results['importance'])
        
        breakdowns = results['breakdowns']
        if 'weekly' in breakdowns:
            all_weekly.append(breakdowns['weekly'])
        if 'sub_customer_name' in breakdowns:
            all_customer.append(breakdowns['sub_customer_name'])
        if 'variety_name' in breakdowns:
            all_variety.append(breakdowns['variety_name'])
    
    # Save combined tables
    if all_predictions:
        save_to_bigquery(pd.concat(all_predictions, ignore_index=True), 'ml_predictions_python')
    
    if all_metrics:
        save_to_bigquery(pd.concat(all_metrics, ignore_index=True), 'ml_model_comparison')
    
    if all_importance:
        save_to_bigquery(pd.concat(all_importance, ignore_index=True), 'ml_feature_importance')
    
    if all_weekly:
        save_to_bigquery(pd.concat(all_weekly, ignore_index=True), 'ml_accuracy_by_week')
    
    if all_customer:
        save_to_bigquery(pd.concat(all_customer, ignore_index=True), 'ml_accuracy_by_customer')
    
    if all_variety:
        save_to_bigquery(pd.concat(all_variety, ignore_index=True), 'ml_accuracy_by_variety')
    
    logger.info("\nAll results saved!")


def main():
    parser = argparse.ArgumentParser(description='VegPro ML Pipeline')
    parser.add_argument('--all', action='store_true', help='Run all models')
    parser.add_argument('--demand', action='store_true', help='Run demand model')
    parser.add_argument('--dispatch', action='store_true', help='Run dispatch model')
    parser.add_argument('--production', action='store_true', help='Run production model')
    parser.add_argument('--rejection', action='store_true', help='Run rejection model')
    
    args = parser.parse_args()
    
    # Determine which models to run
    models_to_run = []
    if args.all:
        models_to_run = ['demand', 'dispatch', 'production', 'rejection']
    else:
        if args.demand:
            models_to_run.append('demand')
        if args.dispatch:
            models_to_run.append('dispatch')
        if args.production:
            models_to_run.append('production')
        if args.rejection:
            models_to_run.append('rejection')
    
    if not models_to_run:
        parser.print_help()
        print("\nPlease specify which models to run.")
        return
    
    logger.info(f"VegPro ML Pipeline - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Models to run: {', '.join(models_to_run)}")
    
    # Run pipelines
    all_results = {}
    for model_name in models_to_run:
        results = run_model_pipeline(model_name)
        all_results[model_name] = results
    
    # Save all results
    save_all_results(all_results)
    
    logger.info("\n" + "="*60)
    logger.info("Pipeline Complete!")
    logger.info("="*60)


if __name__ == '__main__':
    main()
