import yfinance as yf
import pandas as pd
import time
import concurrent.futures
import random
import sys
import numpy as np

# --- 1. 翻译字典 (保持 V21 基础，增加新列) ---
INFO_COL_TRANSLATIONS = {
    'address1': '地址1',
    'address2': '地址2',
    'city': '城市',
    'zip': '邮编',
    'country': '国家',
    'phone': '电话',
    'website': '网站',
    'industry': '产业',
    'industryKey': '产业代码',
    'industryDisp': '产业(显示)',
    'sector': '行业',
    'sectorKey': '行业代码',
    'sectorDisp': '行业(显示)',
    'longBusinessSummary': '业务概要',
    'fullTimeEmployees': '全职员工数',
    'auditRisk': '审计风险',
    'boardRisk': '董事会风险',
    'compensationRisk': '薪酬风险',
    'shareHolderRightsRisk': '股东权利风险',
    'overallRisk': '总体风险',
    'previousClose': '昨收价',
    'open': '开盘价',
    'dayLow': '最低价',
    'dayHigh': '最高价',
    'dividendRate': '年股息额',
    'dividendYield': '股息率(%)',
    'payoutRatio': '派息比率',
    'beta': '贝塔系数',
    'trailingPE': '市盈率(TTM)',
    'forwardPE': '市盈率(远期)',
    'volume': '成交量',
    'averageVolume': '平均成交量',
    'marketCap': '市值',
    'fiftyTwoWeekLow': '52周最低',
    'fiftyTwoWeekHigh': '52周最高',
    'priceToSalesTrailing12Months': '市销率(TTM)',
    'fiftyDayAverage': '50日均线',
    'twoHundredDayAverage': '200日均线',
    'profitMargins': '净利率',
    'floatShares': '流通股',
    'sharesOutstanding': '总股本',
    'heldPercentInsiders': '内部持股%',
    'heldPercentInstitutions': '机构持股%',
    'bookValue': '每股净资产',
    'priceToBook': '市净率(PB)',
    'returnOnAssets': '资产回报率(ROA)',
    'returnOnEquity': '权益回报率(ROE)',
    'earningsGrowth': '盈利增长率',
    'revenueGrowth': '营收增长率',
    'grossMargins': '毛利率',
    'operatingMargins': '营业利润率',
    'totalCash': '总现金',
    'totalDebt': '总债务(有息)',
    'totalRevenue': '总营收',
    'ebitda': 'EBITDA',
    'quickRatio': '速动比率',
    'currentRatio': '流动比率',
    'debtToEquity': '债转股(有息)', # Yahoo原版
    'recommendationMean': '分析师评分',
    'recommendationKey': '分析师建议',
    'currentPrice': '当前价格',
    'targetMeanPrice': '目标均价',
    
    # --- 新增自定义列名 ---
    'quantScore': 'V22量化评分',
    'cn_asset_liability_ratio': '资产负债率(%)' # 这是我们要计算的新指标
}

# 财报数据翻译 (行索引)
FINANCIALS_ROW_TRANSLATIONS = {
    'Total Revenue': '财报总营收',
    'Net Income': '净利润',
    'Operating Income': '营业利润',
    'Total Assets': '总资产',       # 计算资产负债率必须
    'Total Liab': '总负债',         # 计算资产负债率必须
    'Total Equity Gross Minority Interest': '股东权益合计',
    'Cash And Cash Equivalents': '现金及等价物',
    'Operating Cash Flow': '经营现金流',
    'Free Cash Flow': '自由现金流',
}

ALL_TRANSLATIONS = {**INFO_COL_TRANSLATIONS, **FINANCIALS_ROW_TRANSLATIONS}

# --- 2. V22 打分与计算函数 ---
def calculate_metrics_and_score(df):
    """
    V22: 
    1. 计算 '资产负债率' (Total Liab / Total Assets)
    2. 执行加权打分 (越低越好)
    """
    print("正在进行 V22 指标计算与打分...")
    
    # 辅助转换函数
    def safe_num(col_name):
        return pd.to_numeric(df.get(col_name), errors='coerce')

    # --- A. 优先计算用户急需的 '资产负债率' ---
    # 确保使用原始英文列名 (因为还没翻译)
    # Yahoo财务数据通常在: 'Total Liab', 'Total Assets'
    
    t_liab = safe_num('Total Liab')
    t_assets = safe_num('Total Assets')
    
    # 计算公式: (总负债 / 总资产) * 100
    # 处理除以零的情况
    df['cn_asset_liability_ratio'] = (t_liab / t_assets * 100).fillna(0)
    
    # 对异常值进行清洗 (例如大于 1000% 的可能是数据错误，或者资不抵债)
    # 这里保留原值，但在打分时处理
    
    # --- B. 准备打分数据 ---
    # 估值
    pe = safe_num('trailingPE')
    fwd_pe = safe_num('forwardPE')
    pb = safe_num('priceToBook')
    
    # 盈利
    roe = safe_num('returnOnEquity')
    profit_margin = safe_num('profitMargins')
    
    # 成长
    rev_growth = safe_num('revenueGrowth')
    earn_growth = safe_num('earningsGrowth')
    
    # 健康 (加入新的资产负债率)
    asset_liab_ratio = df['cn_asset_liability_ratio']
    div_yield = safe_num('dividendYield')
    
    # --- C. 智能处理 ---
    # 1. 智能 PE: 亏损股(PE<0) 设为极大值 9999
    smart_pe = pe.apply(lambda x: x if x > 0 else 9999)
    smart_fwd_pe = fwd_pe.apply(lambda x: x if x > 0 else 9999)
    
    # --- D. 排名 (Rank) ---
    # pct=True 输出百分比排名 (0~1)
    # ascending=True (小的好), ascending=False (大的好)
    
    ranks = {}
    
    # 估值 (越小越好)
    ranks['pe'] = smart_pe.rank(pct=True, ascending=True)
    ranks['pb'] = pb.rank(pct=True, ascending=True)
    
    # 盈利 (越大越好)
    ranks['roe'] = roe.rank(pct=True, ascending=False)
    ranks['margin'] = profit_margin.rank(pct=True, ascending=False)
    
    # 成长 (越大越好)
    ranks['growth'] = earn_growth.rank(pct=True, ascending=False)
    
    # 健康 (资产负债率: 越小越好 / 股息: 越大越好)
    # 注意：资产负债率通常 40%-60% 比较健康，太低可能经营保守，太高风险大。
    # 这里简化逻辑：越低风险越小 -> 排名越好 (Ascending=True)
    ranks['debt'] = asset_liab_ratio.rank(pct=True, ascending=True)
    ranks['div'] = div_yield.rank(pct=True, ascending=False)
    
    # --- E. 加权总分 (越低越好) ---
    total_score = (
        ranks['pe'] * 1.5 +      # 估值权重高
        ranks['roe'] * 1.5 +     # 盈利权重高
        ranks['growth'] * 1.2 +  # 成长权重中
        ranks['debt'] * 1.2 +    # 负债权重提高 (响应您的需求)
        ranks['div'] * 0.8       # 股息作为补充
    )
    
    # 转换为 0-100 的整数 (为了好看，乘10再取整)
    df['quantScore'] = (total_score * 10).fillna(9999).astype('int')
    
    return df

# --- 3. 爬虫核心 (基本不变) ---
def process_ticker(ticker_symbol):
    data = {'symbol': ticker_symbol}
    try:
        tk = yf.Ticker(ticker_symbol)
        
        # 1. Info
        try:
            info = tk.info
            if info and 'marketCap' in info:
                data.update(info)
            else:
                return None
        except:
            return None
            
        # 2. Balance Sheet (关键：获取总资产和总负债)
        try:
            bs = tk.balance_sheet
            if not bs.empty:
                # 只取最近一期
                latest = bs.iloc[:, 0]
                # 转换时间索引名为字符串，防止冲突
                # data.update(latest.to_dict()) -> 可能会有 Key 冲突，使用特定更新
                for idx, val in latest.items():
                    data[str(idx)] = val
        except:
            pass
            
        # 3. Financials (利润表)
        try:
            fin = tk.financials
            if not fin.empty:
                latest_fin = fin.iloc[:, 0]
                for idx, val in latest_fin.items():
                    data[str(idx)] = val
        except:
            pass
            
        time.sleep(random.uniform(0.5, 1.5))
        return data
        
    except:
        return None

# --- 4. 主流程 ---
def run_scraper(tickers, filename):
    print(f"开始抓取 {len(tickers)} 只股票...")
    
    results = []
    # 建议根据电脑性能调整 workers，太高容易被封 IP
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_ticker, t): t for t in tickers}
        
        count = 0
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res:
                results.append(res)
            count += 1
            if count % 50 == 0:
                print(f"进度: {count}/{len(tickers)} (成功: {len(results)})")
                
    if not results:
        print("未获取到数据。")
        return
        
    # --- 数据处理 ---
    df = pd.DataFrame(results)
    
    # 1. 计算指标与打分 (V22)
    try:
        df = calculate_metrics_and_score(df)
    except Exception as e:
        print(f"打分计算出错: {e}, 将保存原始数据。")
        
    # 2. 翻译列名
    df_renamed = df.rename(columns=ALL_TRANSLATIONS)
    
    # 3. 整理列顺序 (把关键列放前面)
    # 关键列: 代码, 简称, 评分, 资产负债率, 市盈率, 市值
    priority_cols = ['代码', '公司简称', 'V22量化评分', '资产负债率(%)', '市盈率(TTM)', '市值', '股息率(%)']
    
    # 找出实际存在的列
    final_cols = [c for c in priority_cols if c in df_renamed.columns]
    # 把剩下的列加在后面
    remaining_cols = [c for c in df_renamed.columns if c not in final_cols]
    final_cols.extend(remaining_cols)
    
    df_final = df_renamed[final_cols]
    
    # 4. 排序 (按评分)
    if 'V22量化评分' in df_final.columns:
        df_final = df_final.sort_values('V22量化评分')
        
    # 5. 保存
    df_final.to_excel(filename, index=False)
    print(f"\n完成！文件已保存为: {filename}")
    print(f"包含了新指标: '资产负债率(%)'")

if __name__ == "__main__":
    # 生成代码列表 (示例: 前 100 个)
    # 实际运行时请改为 (1, 5000)
    ticker_list = [f"{i:04d}.HK" for i in range(1, 100)] 
    
    # 获取参数或默认文件名
    output_file = sys.argv[1] if len(sys.argv) > 1 else f"HK_Stocks_V22_{time.strftime('%H%M')}.xlsx"
    
    run_scraper(ticker_list, output_file)
