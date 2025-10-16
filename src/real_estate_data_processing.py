import pandas as pd
import geocoder
from geopy.geocoders import Nominatim
import time
import re



# 台灣經緯度範圍
TW_LAT_MIN, TW_LAT_MAX = 21.0, 26.5
TW_LNG_MIN, TW_LNG_MAX = 117.0, 123.8

def normalize_address(address: str) -> str:
    """
    地址清理函數
    - 保留到「幾號」為止
    - 移除括號內補充說明
    """
    if pd.isna(address):
        return address
    
    # 保留到「號」為止
    m = re.search(r".*?號", address)
    base = m.group(0) if m else address
    
    # 移除括號內補充說明
    return re.sub(r"（.*?）|\(.*?\)", "", base).strip()

def is_valid_taiwan_coordinate(lat, lng):
    """檢查經緯度是否在台灣範圍內"""
    if lat is None or lng is None:
        return False
    return TW_LAT_MIN <= lat <= TW_LAT_MAX and TW_LNG_MIN <= lng <= TW_LNG_MAX

def get_coordinates(address):
    """取得經緯度（單次嘗試）"""
    try:
        g = geocoder.arcgis(address, timeout=60)
        r = g.json
        
        if r is not None and r.get('lat') and r.get('lng'):
            lat, lng = r.get('lat'), r.get('lng')
            if is_valid_taiwan_coordinate(lat, lng):
                return lat, lng
            else:
                print(f"座標超出台灣範圍: {lat}, {lng}")
                return None, None
                
    except Exception as e:
        print(f"經緯度轉換異常: {e}")
    
    return None, None

def get_village(lat, lng, geolocator):
    """取得村里別（單次嘗試）"""
    try:
        location = geolocator.reverse(f"{lat}, {lng}")
        
        if location and 'address' in location.raw:
            address_info = location.raw['address']
            village = (address_info.get('neighbourhood') or
                      address_info.get('suburb') or
                      address_info.get('village') or
                      address_info.get('town'))
            return village
            
    except Exception as e:
        print(f"村里轉換異常: {e}")
    
    return None

def format_time(seconds):
    """將秒數轉換為小時分鐘秒數格式"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    if hours > 0:
        return f"{hours}小時{minutes}分{secs}秒"
    elif minutes > 0:
        return f"{minutes}分{secs}秒"
    else:
        return f"{secs}秒"

print("開始處理資料")
print("讀取資料")

# data_a = pd.read_csv("/Users/ylin/Documents/vivian_lab/1124/q_lvr_land_a.csv", encoding='utf-8', skiprows=[1])
# data_a = pd.read_csv("/Users/ylin/Documents/vivian_lab/1124/t_lvr_land_a.csv", encoding='utf-8', skiprows=[1])
# data_a = pd.read_csv("/Users/ylin/Documents/vivian_lab/1124/u_lvr_land_a.csv", encoding='utf-8', skiprows=[1])
data_a = pd.read_csv("/Users/ylin/Documents/vivian_lab/1131/p_lvr_land_a.csv", encoding='utf-8', skiprows=[1])

print(f"成功讀取 {len(data_a)} 筆資料")

# 移除多餘欄位
if 'The villages and towns urban district' in data_a.columns:
    data_a = data_a.drop(['The villages and towns urban district'], axis=1)
    print("已移除多餘的欄位")

# 檢查是否有交易標的欄位
has_transaction_type = '交易標的' in data_a.columns

# 地址清理
print("開始地址清理...")
data_a['土地位置建物門牌'] = data_a['土地位置建物門牌'].apply(normalize_address)
print("地址清理完成")

# 初始化結果列表和失敗重試列表
lat_list = [None] * len(data_a)
lng_list = [None] * len(data_a)
vil_list = [None] * len(data_a)

failed_coordinates = []  # 儲存經緯度轉換失敗的索引
failed_villages = []     # 儲存村里轉換失敗的索引

print("地址轉經緯度和村里別")

# 初始化 Nominatim
geolocator = Nominatim(user_agent="geotest")

start_time = time.time()
processed_count = 0
skipped_land_count = 0

print("=== 第一輪處理 ===")
for i, row in data_a.iterrows():
    address = row['土地位置建物門牌']
    
    # 檢查是否為土地交易（如果有交易標的欄位）
    if has_transaction_type and row['交易標的'] == '土地':
        skipped_land_count += 1
        print(f"第 {i+1} 筆為土地交易，跳過地理編碼")
        continue
    
    processed_count += 1
    
    # ArcGIS 地理編碼
    lat, lng = get_coordinates(address)
    
    if lat and lng:
        print(f"{address} -> {lat}, {lng}")
        lat_list[i] = lat
        lng_list[i] = lng
        
        # Nominatim 反向地理編碼 (1.2秒間隔)
        # time.sleep(1.2)  # Nominatim API 間隔
        time.sleep(0.9)
        village = get_village(lat, lng, geolocator)
        
        if village:
            vil_list[i] = village
            print(f"第 {i+1} 筆完成，村里：{village}")
        else:
            failed_villages.append(i)
            print(f"第 {i+1} 筆完成，村里轉換失敗")
        
    else:
        failed_coordinates.append(i)
        print(f"第 {i+1} 筆地址轉換失敗")
    
    # ArcGIS API 間隔控制
    time.sleep(0.25)  # ArcGIS API 間隔
    
    # # 每10筆顯示進度
    # if (i + 1) % 10 == 0:
    #     print(f"已處理 {i+1} 筆")

print(f"\n=== 第一輪完成 ===")
print(f"經緯度轉換失敗: {len(failed_coordinates)} 筆")
print(f"村里轉換失敗: {len(failed_villages)} 筆")

# 重試失敗的經緯度轉換
if failed_coordinates:
    print(f"\n=== 重試經緯度轉換 ({len(failed_coordinates)} 筆) ===")
    retry_failed_coordinates = []
    
    for i, index in enumerate(failed_coordinates):
        address = data_a.iloc[index]['土地位置建物門牌']
        print(f"重試第 {i+1}/{len(failed_coordinates)} 筆: {address}")
        
        lat, lng = get_coordinates(address)
        
        if lat and lng:
            print(f"重試成功: {address} -> {lat}, {lng}")
            lat_list[index] = lat
            lng_list[index] = lng
            
            # 為重試成功的地址也嘗試取得村里
            time.sleep(1)  # Nominatim API 間隔
            village = get_village(lat, lng, geolocator)
            
            if village:
                vil_list[index] = village
                print(f"村里：{village}")
            else:
                failed_villages.append(index)
                print(f"村里轉換失敗")
        else:
            retry_failed_coordinates.append(index)
            print(f"重試仍失敗")
        
        time.sleep(0.25)  # ArcGIS API 間隔
    
    failed_coordinates = retry_failed_coordinates

# 重試失敗的村里轉換
if failed_villages:
    print(f"\n=== 重試村里轉換 ({len(failed_villages)} 筆) ===")
    retry_failed_villages = []
    
    for i, index in enumerate(failed_villages):
        # 只重試有經緯度的項目
        if lat_list[index] is not None and lng_list[index] is not None:
            lat, lng = lat_list[index], lng_list[index]
            print(f"重試村里第 {i+1}/{len(failed_villages)} 筆: {lat}, {lng}")
            
            time.sleep(1)  # Nominatim API 間隔
            village = get_village(lat, lng, geolocator)
            
            if village:
                vil_list[index] = village
                print(f"重試成功，村里：{village}")
            else:
                retry_failed_villages.append(index)
                print(f"重試仍失敗")
        else:
            print(f"跳過第 {i+1} 筆（無經緯度）")
    
    failed_villages = retry_failed_villages

end_time = time.time()
total_time = end_time - start_time
print(f"\n執行時間：{format_time(total_time)}")

# 將新欄位插入到指定位置
# 找到土地位置建物門牌欄位的位置
address_col_index = data_a.columns.get_loc('土地位置建物門牌')

# 將新欄位資料加入 DataFrame
new_data = data_a.copy()
new_data.insert(address_col_index + 1, '緯度', lat_list)
new_data.insert(address_col_index + 2, '經度', lng_list)
new_data.insert(address_col_index + 3, '村里', vil_list)

print("\n===處理結果===")
print(f"總筆數: {len(data_a)}")
if has_transaction_type:
    print(f"土地交易跳過筆數: {skipped_land_count}")
print(f"實際處理筆數: {processed_count}")
print(f"成功取得經緯度筆數: {len([x for x in lat_list if x is not None])}")
print(f"成功取得村里筆數: {len([x for x in vil_list if x is not None])}")
print(f"最終失敗經緯度筆數: {len(failed_coordinates)}")
print(f"最終失敗村里筆數: {len(failed_villages)}")

# 顯示失敗的地址
if failed_coordinates:
    print(f"\n===經緯度轉換失敗的地址 ({len(failed_coordinates)} 筆)===")
    for i, index in enumerate(failed_coordinates[:]):  # 只顯示前10筆
        address = data_a.iloc[index]['土地位置建物門牌']
        print(f"{i+1}. 第{index+1}筆: {address}")
    if len(failed_coordinates) > 10:
        print(f"... 還有 {len(failed_coordinates) - 10} 筆")

if failed_villages:
    print(f"\n===村里轉換失敗的地址 ({len(failed_villages)} 筆)===")
    for i, index in enumerate(failed_villages[:]):  # 只顯示前10筆
        address = data_a.iloc[index]['土地位置建物門牌']
        lat, lng = lat_list[index], lng_list[index]
        print(f"{i+1}. 第{index+1}筆: {address} (座標: {lat}, {lng})")
    if len(failed_villages) > 10:
        print(f"... 還有 {len(failed_villages) - 10} 筆")

# 儲存結果
# output_file = '/Users/ylin/Documents/vivian_lab/1124_done/q_lvr_land_a_cor.csv'
# output_file = '/Users/ylin/Documents/vivian_lab/1124_done/t_lvr_land_a_cor.csv'
output_file = '/Users/ylin/Documents/vivian_lab/1131_done/p_lvr_land_a_cor.csv' 

new_data.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"\n資料已儲存至: {output_file}")

print("\n===處理結果預覽===")
preview_columns = ['土地位置建物門牌', '緯度', '經度', '村里']
if has_transaction_type:
    preview_columns = ['交易標的'] + preview_columns
available_columns = [col for col in preview_columns if col in new_data.columns]
print(new_data[available_columns].head())