import pandas as pd
import numpy as np
from pathlib import Path
import re
from datetime import datetime, timedelta

class WeatherDataProcessor:
    def __init__(self):
        
        self.special_codes_1_11 = [-9991, -9996, -9997, -9998, -9999]
        self.special_codes_12 = [-999.1, -9995.0, -99.6, -99.1, -9.6, -999.6, -9.5, -99.5, -999.5, 
                                 -9.7, -99.7, -999.7, -9.8, 'None', None]
    
    def clean_value(self, value, month, keep_special_flag=False):
        """清理單一數值，保留特殊代碼標記以供後續處理"""
        if pd.isna(value) or value == 'None':
            return np.nan
        
        try:
            val = float(value)
            if month <= 11:
                if val == -9996:
                    return 'RAW_9996' if keep_special_flag else np.nan
                elif val in [-9991, -9997, -9999]:
                    return 'SPECIAL_MISSING' if keep_special_flag else np.nan
                elif val == -9998:  # 雨跡
                    return 'TRACE' if keep_special_flag else 0.0
            else:  # 12月
                if val == -99.6:  # 12月的-9996
                    return 'RAW_9996' if keep_special_flag else np.nan
                elif val in [-999.1, -99.1, -999.7, -99.7, -9995.0]:
                    return 'SPECIAL_MISSING' if keep_special_flag else np.nan
                elif val == -9.8:  # 12月的雨跡
                    return 'TRACE' if keep_special_flag else 0.0
                elif val in [-9.6, -999.6, -9.5, -99.5, -999.5]:
                    return 'RAW_9996' if keep_special_flag else np.nan
            return val
        except:
            return np.nan
    
    def read_monthly_file(self, filepath, month):
        """讀取單月氣象檔案"""
        data = []
        
        encodings = ['big5', 'cp950', 'gbk', 'utf-8']
        file_content = None
        
        for encoding in encodings:
            try:
                with open(filepath, 'r', encoding=encoding) as f:
                    file_content = f.readlines()
                break
            except UnicodeDecodeError:
                continue
        
        if file_content is None:
            print(f"無法讀取檔案 {filepath}")
            return pd.DataFrame()
        
        for line in file_content:
            if line.startswith('*') or line.startswith('#'):
                continue
            
            line = line.strip()
            if not line:
                continue
            
            try:
                stno = line[0:6].strip()
                datetime_str = line[7:17].strip()

                if month <= 11:
                    fields = []
                    pos = 18
                    while pos < len(line):
                        field = line[pos:pos+7].strip()
                        fields.append(field if field else np.nan)
                        pos += 7
                    
                    if len(fields) >= 6:
                        pp01_raw = fields[5]
                        pp01 = self.clean_value(fields[5], month, keep_special_flag=True)
                        tx01 = self.clean_value(fields[1], month)
                    else:
                        pp01, tx01 = np.nan, np.nan
                        pp01_raw = np.nan
                else: 
                    fields = line[18:].split()
                    if len(fields) >= 8:
                        tx01 = self.clean_value(fields[1], month)
                        pp01_raw = fields[7]
                        pp01 = self.clean_value(fields[7], month, keep_special_flag=True)
                    else:
                        pp01, tx01 = np.nan, np.nan
                        pp01_raw = np.nan

                if len(datetime_str) == 10:
                    year = int(datetime_str[0:4])
                    month_num = int(datetime_str[4:6])
                    day = int(datetime_str[6:8])
                    hour = int(datetime_str[8:10])
                    
                    if month <= 11:
                        display_hour = hour
                        dt_hour = hour - 1 if hour <= 24 else hour
                        if dt_hour == 24:
                            dt_hour = 23
                    else:
                        display_hour = hour + 1
                        dt_hour = hour
                        if hour == 23:
                            display_hour = 24
                    
                    try:
                        dt = datetime(year, month_num, day, dt_hour)
                    except ValueError:
                        continue
                    
                    data.append({
                        'stno': stno,
                        'year': year,
                        'month': month_num,
                        'day': day,
                        'hour': display_hour,
                        'datetime': dt,
                        'PP01': pp01,
                        'PP01_raw': pp01_raw,
                        'TX01': tx01
                    })
            
            except Exception as e:
                print(f"解析錯誤 - 行: {line[:50]}..., 錯誤: {e}")
                continue
        
        return pd.DataFrame(data)
    
    def apply_rainfall_outlier_removal(self, df):
        """降雨量異常值剔除：>220 mm/hr設為NA"""
        df_copy = df.copy()
        
        numeric_mask = pd.to_numeric(df_copy['PP01'], errors='coerce').notna()
        outlier_mask = pd.to_numeric(df_copy['PP01'], errors='coerce') > 220
        
        df_copy.loc[numeric_mask & outlier_mask, 'PP01'] = np.nan
        
        outlier_count = (numeric_mask & outlier_mask).sum()
        if outlier_count > 0:
            print(f"發現且移除 {outlier_count} 筆降雨量異常值 (>220 mm/hr)")
        
        return df_copy
    
    def ensure_complete_hourly_data(self, df):
        """確保每個測站每天都有24小時完整資料"""
        stations = df['stno'].unique()
        start_date = datetime(2023, 1, 1, 0)
        end_date = datetime(2023, 12, 31, 23)
        
        complete_data = []
        
        for station in stations:
            print(f"處理測站 {station}")
            station_data = df[df['stno'] == station].copy()
            
            current_time = start_date
            while current_time <= end_date:
                display_hour = current_time.hour + 1
                
                existing = station_data[
                    (station_data['year'] == current_time.year) &
                    (station_data['month'] == current_time.month) &
                    (station_data['day'] == current_time.day) &
                    (station_data['hour'] == display_hour)
                ]
                
                if len(existing) > 0:
                    record = existing.iloc[0].copy()
                else:
                    record = pd.Series({
                        'stno': station,
                        'year': current_time.year,
                        'month': current_time.month,
                        'day': current_time.day,
                        'hour': display_hour,
                        'datetime': current_time,
                        'PP01': np.nan,
                        'PP01_raw': np.nan,
                        'TX01': np.nan
                    })
                
                complete_data.append(record)
                current_time += timedelta(hours=1)
        
        complete_df = pd.DataFrame(complete_data)
        print(f"完整資料共有 {len(complete_df)} 筆")
        
        return complete_df
    
    def process_special_codes_globally(self, df):
        """全域處理所有特殊代碼，在日聚合前執行"""
        # print("開始處理特殊代碼")
        df_processed = df.copy()
        
        # 先處理所有-9996值
        df_processed = self.process_all_9996_values(df_processed)
        
        # 然後將其他特殊代碼轉為NA
        special_codes = ['SPECIAL_MISSING', 'TRACE']
        for code in special_codes:
            if code == 'TRACE':
                # 雨跡轉為0
                df_processed.loc[df_processed['PP01'] == code, 'PP01'] = 0.0
            else:
                # 其他特殊代碼轉為NA
                df_processed.loc[df_processed['PP01'] == code, 'PP01'] = np.nan
        
        print("特殊代碼處理完成")
        return df_processed
    
    def process_all_9996_values(self, df):
        """處理所有-9996值，按測站分組處理"""
        df_processed = df.copy()
        
        for station in df['stno'].unique():
            station_mask = df_processed['stno'] == station
            station_data = df_processed[station_mask].copy().sort_values('datetime')
            
            # 處理該測站的-9996值
            processed_pp01 = self.process_9996_sequence(station_data['PP01'].tolist())
            
            # 更新資料
            df_processed.loc[station_mask, 'PP01'] = processed_pp01
        
        return df_processed
    
    def process_9996_sequence(self, pp01_values):
        """處理-9996序列"""
        processed_values = []
        i = 0
        
        while i < len(pp01_values):
            if pp01_values[i] == 'RAW_9996':
                start_idx = i
                # 找到-9996序列的結束
                while i < len(pp01_values) and pp01_values[i] == 'RAW_9996':
                    i += 1
                end_idx = i - 1
                
                # 檢查恢復後的第一筆數據
                if i < len(pp01_values):
                    next_value = pp01_values[i]
                    
                    if isinstance(next_value, (int, float)) and not pd.isna(next_value):
                        if next_value > 0:
                            replacement = np.nan  # 情況1：>0，-9996視為NA
                        elif next_value == 0:
                            replacement = 0.0     # 情況2：=0，-9996視為0
                        else:
                            replacement = np.nan  # 情況3：<0，-9996視為NA
                    elif pd.isna(next_value):
                        replacement = np.nan      # 情況4：NA，-9996視為NA
                    elif isinstance(next_value, str):
                        replacement = np.nan      # 情況5：其他特殊代碼，-9996視為NA
                    else:
                        replacement = np.nan
                else:
                    replacement = np.nan          # 末端-9996，視為NA
                
                # 替換整個-9996序列
                for idx in range(start_idx, end_idx + 1):
                    processed_values.append(replacement)
            else:
                processed_values.append(pp01_values[i])
                i += 1
        
        return processed_values
    
    def process_all_months(self, data_folder):
        """處理所有月份資料"""
        all_data = []
        
        # 處理1-11月
        for month in range(1, 12):
            filename = f"2023{month:02d}99.auto_hr.txt"
            filepath = Path(data_folder) / filename
            
            if filepath.exists():
                print(f"處理 {month} 月資料")
                monthly_data = self.read_monthly_file(filepath, month)
                all_data.append(monthly_data)
            else:
                print(f"找不到檔案: {filename}")
        
        # 處理12月
        filename = "20231299.auto_hr.txt"
        filepath = Path(data_folder) / filename
        if filepath.exists():
            print("處理 12 月資料")
            monthly_data = self.read_monthly_file(filepath, 12)
            all_data.append(monthly_data)
        else:
            print(f"找不到檔案: {filename}")
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            # 初步清理
            cleaned_df = self.final_cleaning(combined_df)
            # 降雨量異常值處理
            cleaned_df = self.apply_rainfall_outlier_removal(cleaned_df)
            # 確保完整的小時資料
            complete_df = self.ensure_complete_hourly_data(cleaned_df)
            # 處理所有特殊代碼（包括-9996）
            processed_df = self.process_special_codes_globally(complete_df)
            return processed_df
        else:
            return pd.DataFrame()
    
    def final_cleaning(self, df):
        """最終資料清理"""
        df.loc[df['TX01'] < -30, 'TX01'] = np.nan
        df.loc[df['TX01'] > 50, 'TX01'] = np.nan
        df = df.sort_values(['stno', 'datetime']).reset_index(drop=True)
        return df
    
    def create_daily_data(self, hourly_df):
        """將小時資料聚合為日資料"""
        daily_data = []
        stations = hourly_df['stno'].unique()
        
        for station in stations:
            station_hourly = hourly_df[hourly_df['stno'] == station]
            
            start_date = datetime(2023, 1, 1)
            for day_offset in range(365):
                current_date = start_date + timedelta(days=day_offset)
                year, month, day = current_date.year, current_date.month, current_date.day
                
                day_data = station_hourly[
                    (station_hourly['year'] == year) &
                    (station_hourly['month'] == month) &
                    (station_hourly['day'] == day)
                ]
                
                if len(day_data) > 0:
                    # 降雨量聚合邏輯：只有當全部24小時都是NA時，該日才為NA
                    rainfall_values = day_data['PP01']
                    valid_rainfall = rainfall_values.dropna()
                    
                    if len(valid_rainfall) > 0:
                        # 有任何有效數據（包括0），該日就有數值
                        daily_rainfall = valid_rainfall.sum()
                    else:
                        # 全部24小時都是NA
                        daily_rainfall = np.nan
                    
                    # 溫度(計算有效值的平均)
                    temp_values = day_data['TX01'].dropna()
                    daily_temp = temp_values.mean() if len(temp_values) > 0 else np.nan
                    
                    valid_rainfall_count = len(valid_rainfall)
                    missing_rainfall_count = len(rainfall_values) - valid_rainfall_count
                else:
                    daily_rainfall = np.nan
                    daily_temp = np.nan
                    valid_rainfall_count = 0
                    missing_rainfall_count = 24
                
                daily_data.append({
                    'stno': station,
                    'year': year,
                    'month': month,
                    'day': day,
                    'PP01': daily_rainfall,
                    'TX01': daily_temp,
                    'valid_hours_rainfall': valid_rainfall_count,
                    'missing_hours_rainfall': missing_rainfall_count,
                    'total_hours': 24
                })
        
        daily_df = pd.DataFrame(daily_data)
        daily_df['date'] = pd.to_datetime(daily_df[['year', 'month', 'day']])
        return daily_df
    
    def calculate_station_statistics(self, daily_df):
        """計算各測站統計指標"""
        results = []
        
        for stno in daily_df['stno'].unique():
            station_data = daily_df[daily_df['stno'] == stno].copy()
            station_data = station_data.sort_values('date')
            
            stats = {'stno': stno}
            
            if len(station_data) != 365:
                print(f"提醒：測站 {stno} 資料不完整，只有 {len(station_data)} 天")
            
            # 降雨統計
            rainfall = station_data['PP01']
            valid_rainfall = rainfall.dropna()
            na_count = rainfall.isna().sum()
            valid_count = len(valid_rainfall)
            
            # 降雨日數分類
            small_rain_days = ((valid_rainfall >= 0) & (valid_rainfall < 10)).sum()  # 0 ≤ 降雨量 < 10mm
            medium_rain_days = ((valid_rainfall >= 10) & (valid_rainfall <= 80)).sum()  # 10mm ≤ 降雨量 ≤ 80mm
            heavy_rain_days = (valid_rainfall > 80).sum()  # 降雨量 > 80mm
            
            # 驗證分類完整性
            total_classified = small_rain_days + medium_rain_days + heavy_rain_days + na_count
            if total_classified != 365:
                print(f"提醒：測站 {stno} 降雨分類天數不等於365天: {total_classified}")
            
            stats['小雨日數'] = small_rain_days
            stats['中雨日數'] = medium_rain_days 
            stats['大雨日數'] = heavy_rain_days
            stats['缺測日數'] = na_count
            stats['總天數'] = total_classified
            
            # 年平均降雨量：分母為有效日數，不是365
            stats['年平均降雨量'] = valid_rainfall.mean() if valid_count > 0 else np.nan
            
            # 有降雨之年平均降雨量
            rainy_days = valid_rainfall[valid_rainfall > 0]
            stats['有降雨之年平均降雨量'] = rainy_days.mean() if len(rainy_days) > 0 else np.nan
            
            # 1-5月統計（排除NA）
            jan_may = station_data[station_data['month'].isin([1,2,3,4,5])]
            if len(jan_may) > 0:
                jan_may_valid = jan_may['PP01'].dropna()
                
                # 最長濕潤日數 (降雨>0，NA不計入連續)
                wet_series = (jan_may['PP01'] > 0).astype(int)
                wet_series[jan_may['PP01'].isna()] = -1  # NA標記為-1
                stats['1-5月最長濕潤日數'] = self.longest_consecutive_exclude_na(wet_series)
                
                # 最長乾燥日數 (降雨<5mm，NA不計入連續)
                dry_series = (jan_may['PP01'] < 5).astype(int)
                dry_series[jan_may['PP01'].isna()] = -1  # NA標記為-1
                stats['1-5月最長乾燥日數'] = self.longest_consecutive_exclude_na(dry_series)
            else:
                stats['1-5月最長濕潤日數'] = np.nan
                stats['1-5月最長乾燥日數'] = np.nan
            
            # 6-11月統計
            jun_nov = station_data[station_data['month'].isin([6,7,8,9,10,11])]
            if len(jun_nov) > 0:
                jun_nov_valid = jun_nov['PP01'].dropna()
                stats['6-11月總降雨量'] = jun_nov_valid.sum() if len(jun_nov_valid) > 0 else np.nan
                stats['6-11月總降雨日數'] = (jun_nov_valid > 0).sum()
                stats['6-11月暴雨日數'] = (jun_nov_valid > 200).sum()
            else:
                stats['6-11月總降雨量'] = np.nan
                stats['6-11月總降雨日數'] = np.nan
                stats['6-11月暴雨日數'] = np.nan
            
            # 溫度相關統計
            temperature = station_data['TX01'].dropna()
            
            if len(temperature) > 0:
                # 累積溫度
                high_temp = temperature[temperature > 17]
                stats['累積溫度'] = (high_temp - 17).sum() if len(high_temp) > 0 else 0
                
                # 溫度累積效果
                optimal_temp_days = ((temperature >= 27) & (temperature <= 33)).sum()
                stats['溫度累積效果'] = optimal_temp_days
                
                # 溫度連續效果
                optimal_temp_series = ((temperature >= 27) & (temperature <= 33)).astype(int)
                consecutive_days = self.get_all_consecutive_lengths(optimal_temp_series)
                
                if consecutive_days:
                    stats['溫度平均連續效果'] = np.mean(consecutive_days)
                    stats['溫度最大連續效果'] = max(consecutive_days)
                else:
                    stats['溫度平均連續效果'] = 0
                    stats['溫度最大連續效果'] = 0
                
                # 週均溫>18°C的週數
                weekly_temp = temperature.groupby(temperature.index // 7).mean()
                stats['週均溫大於18度之週數'] = (weekly_temp > 18).sum()
            else:
                stats['累積溫度'] = np.nan
                stats['溫度累積效果'] = np.nan
                stats['溫度平均連續效果'] = np.nan
                stats['溫度最大連續效果'] = np.nan
                stats['週均溫大於18度之週數'] = np.nan
            
            stats['總日數'] = len(station_data)
            stats['有效降雨日數'] = valid_count
            stats['有效溫度日數'] = station_data['TX01'].notna().sum()
            
            results.append(stats)
        
        return pd.DataFrame(results)
    
    def longest_consecutive_exclude_na(self, binary_series):
        """計算最長連續1的長度，排除-1（NA）"""
        if len(binary_series) == 0:
            return 0
        
        max_length = 0
        current_length = 0
        
        for value in binary_series:
            if value == 1:
                current_length += 1
                max_length = max(max_length, current_length)
            elif value == -1:
                # 遇到NA，重置連續計算
                current_length = 0
            else:  # value == 0
                current_length = 0
        
        return max_length
    
    def get_all_consecutive_lengths(self, binary_series):
        """取得所有連續1的長度列表"""
        if len(binary_series) == 0:
            return []
        
        lengths = []
        current_length = 0
        
        for value in binary_series:
            if value == 1:
                current_length += 1
            else:
                if current_length > 0:
                    lengths.append(current_length)
                current_length = 0
        
        if current_length > 0:
            lengths.append(current_length)
        
        return lengths

def main():
    processor = WeatherDataProcessor()
    
    data_folder = "/Users/ylin/Documents/vivian_lab/20239999_auto_hr"
    
    print("開始處理氣象資料")
    
    # 1.處理所有月份資料
    hourly_data = processor.process_all_months(data_folder)
    print(f"總共處理 {len(hourly_data)} 筆小時資料")
    
    # 驗證時間格式和完整性
    print(f"小時範圍: {hourly_data['hour'].min()} - {hourly_data['hour'].max()}")
    
    station_hour_counts = hourly_data.groupby('stno').size()
    print(f"各測站小時資料統計:")
    print(f"實際小時數範圍: {station_hour_counts.min()} - {station_hour_counts.max()}")
    
    incomplete_stations = station_hour_counts[station_hour_counts != 8760]
    if len(incomplete_stations) > 0:
        print(f"發現 {len(incomplete_stations)} 個測站資料不完整:")
        for stno, count in incomplete_stations.items():
            print(f"測站 {stno}: {count} 小時 (缺少 {8760-count} 小時)")
    else:
        print(f"所有 {len(station_hour_counts)} 個測站都有完整的8760小時資料")
    
    # 檢查降雨量範圍
    rainfall_values = pd.to_numeric(hourly_data['PP01'], errors='coerce')
    max_rainfall = rainfall_values.max()
    print(f"降雨量範圍檢查: 最大值 = {max_rainfall} mm/hr")
    if max_rainfall > 220:
        print("仍有降雨量超過220mm/hr的資料")
    
    # 2. 轉換為日資料
    daily_data = processor.create_daily_data(hourly_data)
    print(f"轉換為 {len(daily_data)} 筆日資料")
    
    # 驗證日資料完整性
    station_day_counts = daily_data.groupby('stno').size()
    print(f"期望每站天數: 365天")
    print(f"實際天數範圍: {station_day_counts.min()} - {station_day_counts.max()}")
    
    incomplete_daily_stations = station_day_counts[station_day_counts != 365]
    if len(incomplete_daily_stations) > 0:
        print(f"發現 {len(incomplete_daily_stations)} 個測站日資料不完整:")
        for stno, count in incomplete_daily_stations.items():
            print(f"測站 {stno}: {count} 天")
    else:
        print(f"所有 {len(station_day_counts)} 個測站都有完整的365天資料")
    
    # 3. 計算各測站統計指標
    station_stats = processor.calculate_station_statistics(daily_data)
    print(f"計算完成 {len(station_stats)} 個測站的統計資料")
    

    print(f"\n降雨天數統計驗證:")
    for idx, row in station_stats.iterrows():
        total_days = row['小雨日數'] + row['中雨日數'] + row['大雨日數'] + row['缺測日數']
        if total_days != 365:
            print(f"測站 {row['stno']}: 總天數 = {total_days} (小雨:{row['小雨日數']}, 中雨:{row['中雨日數']}, 大雨:{row['大雨日數']}, 缺測:{row['缺測日數']})")
    
    hourly_data.to_csv('hourly_weather_data.csv', index=False, encoding='utf-8-sig')
    daily_data.to_csv('daily_weather_data.csv', index=False, encoding='utf-8-sig')
    station_stats.to_csv('station_climate_statistics.csv', index=False, encoding='utf-8-sig')
    
    print("處理完成")
    
    # 顯示統計摘要
    print("\n降雨統計摘要:")
    print(f"小雨日數平均: {station_stats['小雨日數'].mean():.1f}天")
    print(f"中雨日數平均: {station_stats['中雨日數'].mean():.1f}天") 
    print(f"大雨日數平均: {station_stats['大雨日數'].mean():.1f}天")
    print(f"缺測日數平均: {station_stats['缺測日數'].mean():.1f}天")
    print(f"年平均降雨量: {station_stats['年平均降雨量'].mean():.2f}mm")
    
    print("\n數據品質統計:")
    print(f"平均每日有效小時數: {daily_data['valid_hours_rainfall'].mean():.1f}")
    # # print(f"平均每日缺失小時數: {daily_data['missing_hours_
    # print(f"平均每日缺失小時數: {daily_data['missing_hours_rainfall'].mean():.1f}")
    # print(f"平均每日原始-9996小時數: {daily_data['raw_9996_hours'].mean():.1f}")
    
    # # 顯示-9996處理統計
    # total_9996 = daily_data['raw_9996_hours'].sum()
    # if total_9996 > 0:
    #     print(f"總共處理了 {total_9996} 個-9996值")
    
    total_na_rainfall = daily_data['PP01'].isna().sum()
    print(f"總共有 {total_na_rainfall} 天的降雨量為NA")

    total_expected_records = len(station_stats) * 365
    actual_records = len(daily_data)
    print(f"資料完整性: {actual_records}/{total_expected_records} = {actual_records/total_expected_records*100:.1f}%")
    
    # print("\n統計資料存至以下檔案:")
    # print("- hourly_weather_data.csv: 完整小時資料")
    # print("- daily_weather_data.csv: 日聚合資料") 
    # print("- station_climate_statistics.csv: 測站氣候統計")
    
    return hourly_data, daily_data, station_stats

if __name__ == "__main__":
    hourly_data, daily_data, station_stats = main()