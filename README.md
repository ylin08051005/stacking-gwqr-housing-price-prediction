# stacking-gwqr-housing-price-prediction


## Taiwan Real Estate Geocoding and Village Mapping

### Overview

This project aims to convert property transaction addresses from Taiwan’s official **Real Estate Transaction Dataset** into precise **geographic coordinates (latitude and longitude)** and to identify the corresponding **village/subdistrict (村里)** using reverse geocoding.
The script includes Taiwan-specific validation and retry mechanisms to ensure data accuracy.

---

### Features

**Address Normalization** – Cleans and standardizes addresses (removes parentheses and extra text).
**Coordinate Retrieval (ArcGIS API)** – Automatically converts addresses to latitude and longitude.
**Village Mapping (Nominatim API)** – Uses OpenStreetMap’s Nominatim service for reverse geocoding.
**Auto-Retry Mechanism** – Retries failed geocoding or village lookups once.
**Taiwan Range Validation** – Ensures coordinates fall within Taiwan’s geographic boundaries.
**Progress and Timing Display** – Reports execution time and processing progress.
**Output Integration** – Adds new columns (`Latitude`, `Longitude`, `Village`) to the dataset and saves to a CSV file.

---

### Requirements

Install the required Python libraries before running:

```bash
pip install pandas geocoder geopy
```

**Python version:** 3.8 or above

---

### Project Structure

```
geocode_project/
├── geocode_village.py        # Main script (this file)
├── input/
│   └── a_lvr_land_a.csv      # Input data (any county/city file)
└── output/
    └── a_lvr_land_a_cor.csv  # Processed output file
```

---

### Usage

1. **Prepare Input Data**
   Download the real estate transaction CSV file (e.g., `x_lvr_land_a.csv`) from the Ministry of the Interior Open Data platform and place it in the `/input` directory.

2. **Update File Paths in the Script**

   ```python
   data_a = pd.read_csv("input_path", encoding='utf-8', skiprows=[1])
   output_file = 'output_path'
   ```

3. **Run the Script**

   ```bash
   python main.py
   ```

4. **Check the Output**
   The resulting CSV file will include three additional columns:

   * `Latitude`
   * `Longitude`
   * `Village`

---

###  Column Description

| Column Name    | Description                                    |
| -------------- | ---------------------------------------------- |
| 土地位置建物門牌       | Original transaction address                   |
| 緯度 (Latitude)  | Latitude obtained via geocoding                |
| 經度 (Longitude) | Longitude obtained via geocoding               |
| 村里 (Village)   | Village/subdistrict name via reverse geocoding |
| 交易標的           | If “土地” (land only), geocoding is skipped      |

---

### Notes

1. **API Rate Limits**

   * ArcGIS and Nominatim APIs have rate limits.
   * The script includes built-in delays: 0.25 seconds for ArcGIS, ~0.9 seconds for Nominatim.
   * Processing large datasets (>1000 records) may take several hours.

2. **Address Quality Matters**

   * Incomplete or ambiguous addresses (e.g., only township names) may fail to return valid coordinates.

3. **Output Encoding**

   * The output CSV file is saved with `utf-8-sig` encoding to prevent garbled Chinese characters.

4. **Retry Mechanism**

   * The script retries failed coordinate or village lookups once to improve success rates.

---

### Sample Output Preview

```text
=== Result Preview ===
      交易標的         土地位置建物門牌           緯度          經度     村里
0   房地(土地+建物)  台中市西屯區文華路100號  24.1782  120.6463  西屯里
1   房地(土地+建物)  台中市南屯區公益路500號  24.1441  120.6485  向心里
2   土地           台中市大里區塗城路80號   None      None     None
...
```

---

### Future Improvements

* Support for **multithreaded processing** to speed up geocoding.
* Add **Google Maps API** as a backup geocoding source.
* Implement **detailed logging** for failed cases and retries.
* Provide **visual map output** using `folium`.

---

###  Author

**Author:** Yi-hsuan Lin (林沂萱)
**Last Updated:** October 2025
