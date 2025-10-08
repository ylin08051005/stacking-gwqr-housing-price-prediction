# Housing Price Prediction using Stacked Ensemble Learning and Geographically Weighted Quantile Regression (GWQR)

## Overview

This project aims to analyze **post-pandemic housing price dynamics in Taipei City** by integrating **Stacked Ensemble Learning** and **Geographically Weighted Quantile Regression (GWQR)**.
The hybrid framework explores the **spatial heterogeneity** and **quantile-dependent patterns** of housing prices, comparing its predictive performance against traditional machine learning models.

---

## Research Background

Following the end of Taiwan’s COVID-19 epidemic command on **May 1, 2023**, the real estate market entered a new phase of post-pandemic normalization.
This study focuses on housing transaction data from **May 2023 to December 2024**, analyzing how various **economic**, **demographic**, **environmental**, and **geographic** factors influence Taipei’s housing market under spatial variation.

---

## Methodology

### 1.Data Collection and Preprocessing

The project integrates multiple open data sources:

* **Real Estate Transaction Data** — Ministry of the Interior’s Actual Price Registration
* **Socioeconomic Data** — Government Open Data Platform
* **Meteorological Data** — Central Weather Administration Automatic Weather Stations

Data preprocessing is performed using **Python**, including:

* Address-to-coordinate conversion for spatial analysis (latitude & longitude)
* Feature extraction (building age, floor area, distance to MRT/commercial centers)
* Integration of geographic layers for mapping and visualization
* Outlier removal and data normalization

Two main preprocessing modules are implemented:

**Real Estate Data Converter** — transforms actual price data into structured tabular form.
**Weather Data Processor** — aggregates hourly weather data into daily/monthly statistics, computes rainfall and temperature indices, and fills temporal gaps.

---

### 2.Variable Selection

To enhance model efficiency and interpretability, multiple variable selection methods are applied:

* **Stepwise regression** and **backward elimination** (traditional statistical approaches)
* **Recursive Feature Elimination (RFE)** based on Random Forests
  The most suitable subset of predictors is selected after comparing the performance of different methods.

---

### 3.Stacking Ensemble Framework

#### **Architecture**

The proposed model is a **two-layer stacked ensemble** combining traditional ML and spatial quantile regression:

```
|-------------------|
|  Meta Learner     | → GWQR (analyzes spatial & quantile effects)
|-------------------|
        ↑
|-------------------|
|  Base Learners    | → RF, XGBoost, LightGBM, CNN
|-------------------|
```

#### **First Layer: Base Learners**

Each base learner independently predicts housing prices across four quantiles (τ = 0.25, 0.50, 0.75, 0.90):

1. **Random Forest (RF)** – captures nonlinear relations and feature importance
2. **XGBoost** – gradient boosting with strong regularization
3. **LightGBM** – lightweight, high-speed boosting framework
4. **Convolutional Neural Network (CNN)** – extracts local and nonlinear patterns

Each model undergoes **hyperparameter tuning** using **grid search** or **Bayesian optimization** combined with **K-fold cross-validation**.

#### **Second Layer: Meta Learner (GWQR)**

GWQR (Geographically Weighted Quantile Regression) integrates spatial dependence and quantile-level variation.
The model estimates local regression coefficients β(u,v) for each geographic location (u,v), enabling the analysis of **price heterogeneity across space** and **different market tiers**.

Mathematically, for quantile τ (0 < τ < 1):

$$
Y_i = X_i^T \beta_\tau(u_i, v_i) + \varepsilon_i
$$

where the coefficients vary by location, and weights are assigned via a spatial kernel function ( K(d_{ij}, h) ) based on distance and bandwidth ( h ).

---

### 4.Model Training Process

1. Split training data into *K* folds for cross-validation
2. Train base models on the same dataset to produce quantile-specific predictions
3. Feed base model outputs and spatial coordinates into GWQR
4. Generate final predictions with quantile-dependent spatial effects

---

## Evaluation Metrics

Model performance is assessed through multiple indicators:

* **RMSE** — Root Mean Square Error
* **Total Check Loss** — Quantile regression loss measure
* **Pseudo-R²** — Goodness-of-fit for quantile models
* **sMAPE** — Symmetric Mean Absolute Percentage Error
* **Moran’s I** — Spatial autocorrelation measure

Results will be compared among:

* Single traditional ML models
* GWR and GWQR alone
* Proposed Stacking-GWQR hybrid model

---

## Visualization

Spatial distributions of predicted prices across quantiles (25%, 50%, 75%, 90%) are visualized using **ArcGIS** or **QGIS**, highlighting spatial disparities and local market patterns in Taipei City.
