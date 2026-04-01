import yfinance as yf
import pandas as pd
import time
import concurrent.futures
import random
import sys

# --- 目标字段及其对应中文翻译 ---
# 严格按照需求：市净率、市盈率、利润率、总市值、总负债、股息率、roe、ROA、经营现金流
TARGET_FIELDS = {
    'symbol': '代码',
    'shortName': '公司简称',
    'priceToBook': '市净率(P/B)',
    'trailingPE': '市盈率(TTM)',
    'profitMargins': '利润率',
    'marketCap': '总市值',
    'totalDebt': '总负债',
    'dividendYield': '股息率',
    'returnOnEquity': 'ROE(净资产收益率)',
    'returnOnAssets': 'ROA(总资产收益率)',
    'operatingCashflow': '经营现金流'
}

def process_ticker(ticker_symbol):
    """为单个股票代码获取指定的指标数据。"""
    combined_data = {'symbol': ticker_symbol}
    
    try:
        ticker = yf.Ticker(ticker_symbol)
        
        # --- 智能重试机制 ---
        MAX_RETRIES = 5
        RETRY_DELAY = 10 # 触发限速时的基础等待时间
        
        info = None
        for attempt in range(MAX_RETRIES):
            try:
                info = ticker.info
                break 
            except Exception as e:
                error_str = str(e)
                if "Too Many Requests" in error_str or "Rate limited" in error_str:
                    wait_time = RETRY_DELAY * (attempt + 1)
                    time.sleep(wait_time)
                elif "404" in error_str or "Not Found" in error_str:
                    return None
                else:
                    if attempt == MAX_RETRIES - 1:
                        return None
                    time.sleep(2)

        if not info:
            return None
            
        # 简单校验数据有效性：如果没有市值，通常说明已退市或没有有效交易数据
        if not info.get('marketCap'): 
            return None
        
        # 提取目标字段
        for key in TARGET_FIELDS.keys():
            if key != 'symbol':
                combined_data[key] = info.get(key, None)

        # 随机休眠防封 (减轻对 Yahoo 服务器的请求压力)
        sleep_time = random.uniform(0.5, 1.5)
        time.sleep(sleep_time)

        return combined_data
        
    except Exception:
        return None


def get_hk_stock_info_combined(tickers, output_filename="hk_stocks_custom_metrics.xlsx"):
    all_combined_data = []
    total_tickers = len(tickers)
    MAX_WORKERS = 8 # 线程数

    print(f"开始并发获取 {total_tickers} 只股票数据...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {executor.submit(process_ticker, ticker): ticker for ticker in tickers}
        
        completed_count = 0
        
        for future in concurrent.futures.as_completed(future_to_ticker):
            completed_count += 1
            
            result = future.result()
            if result:
                all_combined_data.append(result)

            if completed_count % 50 == 0:
                print(f"进度: {completed_count}/{total_tickers} (有效数据: {len(all_combined_data)} 条)")

    print("\n数据获取完毕，正在整理导出...")

    try:
        if all_combined_data:
            df_raw = pd.DataFrame(all_combined_data)
            
            # 重命名列为中文
            df_combined = df_raw.rename(columns=TARGET_FIELDS)
            
            # 确保列顺序按照 TARGET_FIELDS 定义的顺序排列
            ordered_cols = [TARGET_FIELDS[k] for k in TARGET_FIELDS.keys() if TARGET_FIELDS[k] in df_combined.columns]
            df_combined = df_combined[ordered_cols]
            
            # 将股息率和利润率等小数转为百分比易读格式（可选，这里保持原始小数形式，方便Excel二次计算。如果需要可解除注释）
            # df_combined['股息率'] = df_combined['股息率'].apply(lambda x: f"{x*100:.2f}%" if pd.notnull(x) else x)

            # 保存为 Excel
            df_combined.to_excel(output_filename, sheet_name='核心财务指标', index=False)
            print(f"\n成功! 结果已保存至: {output_filename}")
        else:
            print("未获取到有效数据。")

    except Exception as e:
        print(f"保存文件出错: {e}")

def generate_ticker_list(start=1, end=4000, suffix=".HK"):
    """生成港股代码列表，默认 0001.HK 到 4000.HK"""
    print(f"生成代码列表: {start:04d}{suffix} - {end:04d}{suffix}")
    return [f"{i:04d}{suffix}" for i in range(start, end + 1)]

if __name__ == "__main__":
    # 设定抓取范围
    hk_ticker_list = generate_ticker_list(1, 4000) 
    
    if len(sys.argv) > 1:
        output_filename = sys.argv[1]
    else:
        timestamp = time.strftime("%Y%m%d_%H%M", time.localtime())
        output_filename = f"HK_Stocks_CoreMetrics_{timestamp}.xlsx"
    
    get_hk_stock_info_combined(hk_ticker_list, output_filename=output_filename)
