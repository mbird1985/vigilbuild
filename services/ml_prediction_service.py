"""
ML Prediction Service for inventory usage forecasting
"""

import logging
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from config import ML_MODEL_ENABLED
from services.db import db_connection, get_connection, release_connection
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InventoryUsagePrediction:
    """ML service for predicting inventory usage based on job patterns"""
    
    def __init__(self):
        self.model = None
        self.encoders = {}
        self.scaler = StandardScaler()
        self.model_path = 'models/inventory_usage_model.joblib'
        self.encoders_path = 'models/inventory_encoders.joblib'
        self.scaler_path = 'models/inventory_scaler.joblib'
        
        # Ensure models directory exists
        os.makedirs('models', exist_ok=True)
    
    def collect_training_data(self) -> pd.DataFrame:
        """Collect historical job and consumable usage data for training"""
        conn = get_connection()

        try:
            # Query to get job-consumable usage patterns
            query = """
            SELECT 
                j.job_type,
                j.square_footage,
                j.duration_days,
                j.crew_size,
                j.location,
                c.category,
                c.base_name,
                c.specifications,
                jc.quantity_used,
                c.unit,
                EXTRACT(MONTH FROM jc.date_used) as month,
                EXTRACT(YEAR FROM jc.date_used) as year,
                EXTRACT(DOW FROM jc.date_used) as day_of_week
            FROM job_consumables jc
            JOIN jobs j ON jc.job_id = j.id
            JOIN consumables c ON jc.consumable_id = c.id
            WHERE j.status = 'completed'
            AND jc.date_used >= CURRENT_DATE - INTERVAL '2 years'
            AND j.square_footage IS NOT NULL
            AND j.square_footage > 0
            AND jc.quantity_used > 0
            ORDER BY jc.date_used DESC
            """
            
            df = pd.read_sql_query(query, conn)
            
            if df.empty:
                logger.warning("No training data available")
                return df
            
            # Feature engineering
            df['usage_per_sqft'] = df['quantity_used'] / df['square_footage']
            df['usage_per_crew'] = df['quantity_used'] / df['crew_size'].fillna(1)
            
            # Parse specifications JSON to extract key features
            df['spec_features'] = df['specifications'].apply(self._extract_spec_features)
            
            logger.info(f"Collected {len(df)} training records")
            return df

        finally:
            release_connection(conn)
    
    def _extract_spec_features(self, spec_json: str) -> Dict:
        """Extract key features from specifications JSON"""
        try:
            if pd.isna(spec_json) or not spec_json:
                return {}
            
            specs = json.loads(spec_json) if isinstance(spec_json, str) else spec_json
            
            # Extract numerical features
            features = {}
            for key, value in specs.items():
                if isinstance(value, (int, float)):
                    features[f'spec_{key}'] = value
                elif isinstance(value, str):
                    # Try to extract numerical values from strings
                    import re
                    numbers = re.findall(r'\d+\.?\d*', value)
                    if numbers:
                        features[f'spec_{key}_num'] = float(numbers[0])
                    features[f'spec_{key}_cat'] = value
            
            return features
        except:
            return {}
    
    def prepare_features(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
        """Prepare features for training or prediction"""
        if df.empty:
            return df, pd.Series()
        
        # Create feature matrix
        features = []
        
        # Categorical features
        categorical_cols = ['job_type', 'category', 'base_name', 'location', 'unit']
        for col in categorical_cols:
            if col in df.columns:
                if col not in self.encoders:
                    self.encoders[col] = LabelEncoder()
                    features.append(pd.Series(self.encoders[col].fit_transform(df[col].fillna('unknown')), name=col))
                else:
                    # Handle new categories in prediction
                    encoded = []
                    for val in df[col].fillna('unknown'):
                        if val in self.encoders[col].classes_:
                            encoded.append(self.encoders[col].transform([val])[0])
                        else:
                            encoded.append(-1)  # Unknown category
                    features.append(pd.Series(encoded, name=col))
        
        # Numerical features
        numerical_cols = ['square_footage', 'duration_days', 'crew_size', 'month', 'year', 'day_of_week']
        for col in numerical_cols:
            if col in df.columns:
                features.append(df[col].fillna(0))
        
        # Specification features (expanded)
        if 'spec_features' in df.columns:
            spec_df = pd.json_normalize(df['spec_features'])
            for col in spec_df.columns:
                if spec_df[col].dtype in ['int64', 'float64']:
                    features.append(spec_df[col].fillna(0))
        
        X = pd.concat(features, axis=1) if features else pd.DataFrame()
        y = df['usage_per_sqft'] if 'usage_per_sqft' in df.columns else pd.Series()
        
        return X, y
    
    def train_model(self) -> Dict[str, float]:
        """Train the usage prediction model"""
        logger.info("Starting model training...")
        
        # Collect training data
        df = self.collect_training_data()
        if df.empty or len(df) < 50:
            logger.error("Insufficient training data (need at least 50 records)")
            return {'error': 'Insufficient training data'}
        
        # Prepare features
        X, y = self.prepare_features(df)
        if X.empty:
            logger.error("No features could be prepared")
            return {'error': 'Feature preparation failed'}
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train model
        self.model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )
        
        self.model.fit(X_train_scaled, y_train)
        
        # Evaluate model
        y_pred = self.model.predict(X_test_scaled)
        mae = mean_absolute_error(y_test, y_pred)
        mse = mean_squared_error(y_test, y_pred)
        
        # Save model and encoders
        self._save_model()
        
        logger.info(f"Model training completed. MAE: {mae:.4f}, MSE: {mse:.4f}")
        
        return {
            'mae': mae,
            'mse': mse,
            'training_samples': len(X_train),
            'test_samples': len(X_test),
            'features': list(X.columns)
        }
    
    def predict_job_usage(self, job_specs: Dict) -> Dict[str, List[Dict]]:
        """Predict consumable usage for a new job"""
        if not ML_MODEL_ENABLED:
            return {'predictions': [], 'message': 'ML predictions disabled'}
        
        # Load model if not loaded
        if self.model is None:
            if not self._load_model():
                return {'predictions': [], 'error': 'No trained model available'}
        
        try:
            # Get unique consumable categories from training data
            conn = get_connection()
            query = """
            SELECT DISTINCT category, base_name, specifications
            FROM consumables
            WHERE active = TRUE
            ORDER BY category, base_name
            """
            consumables_df = pd.read_sql_query(query, conn)
            release_connection(conn)
            
            if consumables_df.empty:
                return {'predictions': [], 'error': 'No consumables in database'}
            
            predictions = []
            
            for _, consumable in consumables_df.iterrows():
                # Create prediction input
                pred_data = {
                    'job_type': job_specs.get('job_type', 'general'),
                    'square_footage': job_specs.get('square_footage', 1000),
                    'duration_days': job_specs.get('duration_days', 5),
                    'crew_size': job_specs.get('crew_size', 3),
                    'location': job_specs.get('location', 'unknown'),
                    'category': consumable['category'],
                    'base_name': consumable['base_name'],
                    'specifications': consumable['specifications'],
                    'unit': 'each',  # Default unit
                    'month': datetime.now().month,
                    'year': datetime.now().year,
                    'day_of_week': datetime.now().weekday()
                }
                
                # Convert to DataFrame
                pred_df = pd.DataFrame([pred_data])
                pred_df['spec_features'] = pred_df['specifications'].apply(self._extract_spec_features)
                
                # Prepare features
                X_pred, _ = self.prepare_features(pred_df)
                
                if not X_pred.empty:
                    # Scale and predict
                    X_pred_scaled = self.scaler.transform(X_pred)
                    usage_per_sqft = self.model.predict(X_pred_scaled)[0]
                    
                    # Calculate total usage
                    total_usage = max(0, usage_per_sqft * job_specs.get('square_footage', 1000))
                    
                    if total_usage > 0.01:  # Only include meaningful predictions
                        predictions.append({
                            'category': consumable['category'],
                            'base_name': consumable['base_name'],
                            'predicted_quantity': round(total_usage, 2),
                            'confidence': min(1.0, total_usage / 10),  # Simple confidence metric
                            'unit': 'each'
                        })
            
            # Sort by predicted quantity (highest first)
            predictions.sort(key=lambda x: x['predicted_quantity'], reverse=True)
            
            return {
                'predictions': predictions[:20],  # Top 20 predictions
                'job_specs': job_specs,
                'prediction_date': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Prediction error: {str(e)}")
            return {'predictions': [], 'error': str(e)}
    
    def _save_model(self):
        """Save trained model and encoders"""
        self.model_path = f'models/inventory_usage_model_{datetime.now().strftime("%Y%m%d_%H%M")}.joblib'
        joblib.dump(self.model, self.model_path)
        joblib.dump(self.encoders, self.encoders_path)
        joblib.dump(self.scaler, self.scaler_path)
        logger.info("Model and encoders saved successfully")
    
    def _load_model(self) -> bool:
        """Load trained model and encoders"""
        try:
            # Find the latest model file
            model_files = [f for f in os.listdir('models') if f.startswith('inventory_usage_model_') and f.endswith('.joblib')]
            if not model_files:
                logger.warning("No model files found in models directory.")
                return False
            
            latest_model_path = max(model_files, key=lambda x: datetime.strptime(x.replace('inventory_usage_model_', '').replace('.joblib', ''), '%Y%m%d_%H%M'))
            
            if (os.path.exists(os.path.join('models', latest_model_path)) and 
                os.path.exists(self.encoders_path) and 
                os.path.exists(self.scaler_path)):
                
                self.model = joblib.load(os.path.join('models', latest_model_path))
                self.encoders = joblib.load(self.encoders_path)
                self.scaler = joblib.load(self.scaler_path)
                logger.info(f"Loaded model from {latest_model_path}")
                return True
        except Exception as e:
            logger.error(f"Error loading model: {str(e)}")
        
        return False