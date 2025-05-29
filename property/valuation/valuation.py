import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, accuracy_score
from database.database import *
import joblib
import os

class PropertyValuator:
    """
    Provides multiple methods for property valuation including income-based, comparison-based,
    regression model, and classification model approaches.
    """
    
    def __init__(self, db_manager):
        """
        Initializes the PropertyValuator with a database manager instance.
        
        :param db_manager: An instance of DatabaseManager for database operations
        """
        self.db = db_manager
        self.regression_model = None
        self.classification_model = None
        self.scaler = None
        self.model_dir = "models"
        os.makedirs(self.model_dir, exist_ok=True)
        
    def income_based_valuation(self, state, postcode, address, area_name, 
                             property_type, bedroom_num, bathroom_num, parking_num):
        """
        Estimates property value using income approach (rental capitalization).
        
        :param state: State of the property
        :param postcode: Postcode of the property
        :param address: Street address of the property
        :param area_name: Suburb or area name
        :param property_type: Property type code
        :param bedroom_num: Number of bedrooms
        :param bathroom_num: Number of bathrooms
        :param parking_num: Number of parking spaces
        
        :return: Estimated property value or None if insufficient data
        """
        # Get similar rental properties
        rental_properties = self.db.query_k_nearest_properties(
            state=state,
            postcode=postcode,
            address=address,
            area_name=area_name,
            k=10,
            mode=0,  # rental properties
            property_type=property_type,
            bedroom_num=bedroom_num,
            bathroom_num=bathroom_num,
            parking_num=parking_num,
            range_percentage=-1  # no price range filter
        )
        
        if not rental_properties or len(rental_properties) < 3:
            print("Insufficient rental data for valuation")
            return None
            
        # Extract rental prices (7th column is price)
        rental_prices = [prop[6] for prop in rental_properties]
        median_rental = np.median(rental_prices)
        
        # Apply typical capitalization rate (can be adjusted)
        # Typical cap rates vary by market - 5% used as example
        cap_rate = 0.05  
        estimated_value = (median_rental * 12) / cap_rate
        
        return estimated_value
        
    def comparison_based_valuation(self, state, postcode, address, area_name,
                                 property_type, bedroom_num, bathroom_num, parking_num):
        """
        Estimates property value using sales comparison approach.
        
        :param state: State of the property
        :param postcode: Postcode of the property
        :param address: Street address of the property
        :param area_name: Suburb or area name
        :param property_type: Property type code
        :param bedroom_num: Number of bedrooms
        :param bathroom_num: Number of bathrooms
        :param parking_num: Number of parking spaces
        
        :return: Estimated property value or None if insufficient data
        """
        # Get similar sold properties
        sold_properties = self.db.query_k_nearest_properties(
            state=state,
            postcode=postcode,
            address=address,
            area_name=area_name,
            k=20,
            mode=1,  # sold properties
            property_type=property_type,
            bedroom_num=bedroom_num,
            bathroom_num=bathroom_num,
            parking_num=parking_num,
            range_percentage=-1  # no price range filter
        )
        
        if not sold_properties or len(sold_properties) < 5:
            print("Insufficient comparable sales data for valuation")
            return None
            
        # Extract sold prices (7th column is price)
        sold_prices = [prop[6] for prop in sold_properties]
        median_price = np.median(sold_prices)
        
        return median_price
        
    def train_regression_model(self, test_size=0.2, random_state=42):
        """
        Trains a regression model on sold properties data to predict property prices.
        
        :param test_size: Proportion of data to use for testing
        :param random_state: Random seed for reproducibility
        
        :return: Tuple of (training MAE, testing MAE)
        """
        try:
            # Fetch all sold properties data
            self.db.cursor.execute("""
                SELECT 
                    postcode, bedroom_num, bathroom_num, parking_num, 
                    property_type, latitude, longitude, price
                FROM sold_properties
                WHERE inactive = FALSE
            """)
            data = self.db.cursor.fetchall()
            
            if not data or len(data) < 100:
                print("Insufficient data for training regression model")
                return None
                
            # Convert to DataFrame
            df = pd.DataFrame(data, columns=[
                'postcode', 'bedrooms', 'bathrooms', 'parking', 
                'property_type', 'latitude', 'longitude', 'price'
            ])
            
            # Feature engineering
            X = df.drop('price', axis=1)
            y = df['price']
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=test_size, random_state=random_state
            )
            
            # Scale features
            self.scaler = StandardScaler()
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Train model
            self.regression_model = RandomForestRegressor(
                n_estimators=100,
                random_state=random_state
            )
            self.regression_model.fit(X_train_scaled, y_train)
            
            # Evaluate
            train_pred = self.regression_model.predict(X_train_scaled)
            test_pred = self.regression_model.predict(X_test_scaled)
            
            train_mae = mean_absolute_error(y_train, train_pred)
            test_mae = mean_absolute_error(y_test, test_pred)
            
            # Save model
            model_path = os.path.join(self.model_dir, "property_regression_model.pkl")
            joblib.dump(self.regression_model, model_path)
            
            scaler_path = os.path.join(self.model_dir, "property_scaler.pkl")
            joblib.dump(self.scaler, scaler_path)
            
            return train_mae, test_mae
            
        except Exception as e:
            print(f"Error training regression model: {e}")
            return None
            
    def predict_with_regression_model(self, postcode, bedrooms, bathrooms, parking, 
                                    property_type, latitude, longitude):
        """
        Predicts property value using the trained regression model.
        
        :param postcode: Property postcode
        :param bedrooms: Number of bedrooms
        :param bathrooms: Number of bathrooms
        :param parking: Number of parking spaces
        :param property_type: Property type code
        :param latitude: Geographic latitude
        :param longitude: Geographic longitude
        
        :return: Predicted value or None if model not trained
        """
        if not self.regression_model:
            # Try to load saved model
            model_path = os.path.join(self.model_dir, "property_regression_model.pkl")
            scaler_path = os.path.join(self.model_dir, "property_scaler.pkl")
            
            if os.path.exists(model_path) and os.path.exists(scaler_path):
                self.regression_model = joblib.load(model_path)
                self.scaler = joblib.load(scaler_path)
            else:
                print("Regression model not trained. Please train first.")
                return None
                
        # Prepare input data
        input_data = pd.DataFrame([[
            postcode, bedrooms, bathrooms, parking, 
            property_type, latitude, longitude
        ]], columns=[
            'postcode', 'bedrooms', 'bathrooms', 'parking', 
            'property_type', 'latitude', 'longitude'
        ])
        
        # Scale and predict
        input_scaled = self.scaler.transform(input_data)
        prediction = self.regression_model.predict(input_scaled)
        
        return prediction[0]
        
    def train_classification_model(self, price_bins=5, test_size=0.2, random_state=42):
        """
        Trains a classification model on sold properties data to predict price category.
        
        :param price_bins: Number of price categories to create
        :param test_size: Proportion of data to use for testing
        :param random_state: Random seed for reproducibility
        
        :return: Tuple of (training accuracy, testing accuracy)
        """
        try:
            # Fetch all sold properties data
            self.db.cursor.execute("""
                SELECT 
                    postcode, bedroom_num, bathroom_num, parking_num, 
                    property_type, latitude, longitude, price
                FROM sold_properties
                WHERE inactive = FALSE
            """)
            data = self.db.cursor.fetchall()
            
            if not data or len(data) < 100:
                print("Insufficient data for training classification model")
                return None
                
            # Convert to DataFrame
            df = pd.DataFrame(data, columns=[
                'postcode', 'bedrooms', 'bathrooms', 'parking', 
                'property_type', 'latitude', 'longitude', 'price'
            ])
            
            # Create price categories
            df['price_category'] = pd.qcut(
                df['price'], 
                q=price_bins, 
                labels=False, 
                duplicates='drop'
            )
            
            # Feature engineering
            X = df.drop(['price', 'price_category'], axis=1)
            y = df['price_category']
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=test_size, random_state=random_state
            )
            
            # Scale features
            self.scaler = StandardScaler()
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Train model
            self.classification_model = RandomForestClassifier(
                n_estimators=100,
                random_state=random_state
            )
            self.classification_model.fit(X_train_scaled, y_train)
            
            # Evaluate
            train_pred = self.classification_model.predict(X_train_scaled)
            test_pred = self.classification_model.predict(X_test_scaled)
            
            train_acc = accuracy_score(y_train, train_pred)
            test_acc = accuracy_score(y_test, test_pred)
            
            # Save model
            model_path = os.path.join(self.model_dir, "property_classification_model.pkl")
            joblib.dump(self.classification_model, model_path)
            
            return train_acc, test_acc
            
        except Exception as e:
            print(f"Error training classification model: {e}")
            return None
            
    def predict_with_classification_model(self, postcode, bedrooms, bathrooms, parking, 
                                        property_type, latitude, longitude):
        """
        Predicts property price category using the trained classification model.
        
        :param postcode: Property postcode
        :param bedrooms: Number of bedrooms
        :param bathrooms: Number of bathrooms
        :param parking: Number of parking spaces
        :param property_type: Property type code
        :param latitude: Geographic latitude
        :param longitude: Geographic longitude
        
        :return: Predicted price category or None if model not trained
        """
        if not self.classification_model:
            # Try to load saved model
            model_path = os.path.join(self.model_dir, "property_classification_model.pkl")
            
            if os.path.exists(model_path):
                self.classification_model = joblib.load(model_path)
                # Classification uses same scaler as regression
                scaler_path = os.path.join(self.model_dir, "property_scaler.pkl")
                if os.path.exists(scaler_path):
                    self.scaler = joblib.load(scaler_path)
            else:
                print("Classification model not trained. Please train first.")
                return None
                
        # Prepare input data
        input_data = pd.DataFrame([[
            postcode, bedrooms, bathrooms, parking, 
            property_type, latitude, longitude
        ]], columns=[
            'postcode', 'bedrooms', 'bathrooms', 'parking', 
            'property_type', 'latitude', 'longitude'
        ])
        
        # Scale and predict
        input_scaled = self.scaler.transform(input_data)
        prediction = self.classification_model.predict(input_scaled)
        
        return prediction[0]