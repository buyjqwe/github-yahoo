import yfinance as yf
import pandas as pd
import time
import concurrent.futures
import random
import sys
import numpy as np # 引入 numpy 进行更科学的数据处理

# --- 中文翻译字典 (V12 修订版) ---
INFO_COL_TRANSLATIONS = {
    'address1': '地址1',
    'address2': '地址2',
    'city': '城市',
    'zip': '邮编',
    'country': '国家',
    'phone': '电话',
    'fax': '传真',
    'website': '网站',
    'irWebsite': '投资者关系网站',
    'industry': '产业',
    'industryKey': '产业代码',
    'industryDisp': '产业(显示)',
    'sector': '行业',
    'sectorKey': '行业代码',
    'sectorDisp': '行业(显示)',
    'longBusinessSummary': '业务概要',
    'fullTimeEmployees': '全职员工数',
    'companyOfficers': '公司高管',
    'auditRisk': '审计风险',
    'boardRisk': '董事会风险',
    'compensationRisk': '薪酬风险',
    'shareHolderRightsRisk': '股东权利风险',
    'overallRisk': '总体风险',
    'governanceEpochDate': '治理日期',
    'compensationAsOfEpochDate': '薪酬日期',
    'maxAge': '最大时效',
    'priceHint': '价格提示',
    'previousClose': '昨收价',
    'open': '开盘价(Info)',
    'dayLow': '当日最低(Info)',
    'dayHigh': '当日最高(Info)',
    'regularMarketPreviousClose': '正常市昨收价',
    'regularMarketOpen': '正常市开盘价',
    'regularMarketDayLow': '正常市当日最低',
    'regularMarketDayHigh': '正常市当日最高',
    'dividendRate': '年股息额(现金)',
    'dividendYield': '股息率(%)',
    'exDividendDate': '除息日',
    'payoutRatio': '派息比率',
    'fiveYearAvgDividendYield': '五年平均股息率',
    'beta': '贝塔系数',
    'trailingPE': '市盈率(TTM)',
    'forwardPE': '市盈率(远期)',
    'volume': '成交量(Info)',
    'regularMarketVolume': '正常市成交量',
    'averageVolume': '平均成交量',
    'averageVolume10days': '10日平均成交量',
    'averageDailyVolume10Day': '10日平均成交量',
    'bid': '买入价',
    'ask': '卖出价',
    'bidSize': '买入量',
    'askSize': '卖出量',
    'marketCap': '市值',
    'fiftyTwoWeekLow': '52周最低',
    'fiftyTwoWeekHigh': '52周最高',
    'priceToSalesTrailing12Months': '追踪12个月市销率',
    'fiftyDayAverage': '50日均线',
    'twoHundredDayAverage': '200日均线',
    'trailingAnnualDividendRate': '追踪年股息率',
    'trailingAnnualDividendYield': '追踪年股息收益率',
    'currency': '货币',
    'enterpriseValue': '企业价值',
    'profitMargins': '利润率',
    'floatShares': '流通股',
    'sharesOutstanding': '总发行股数',
    'heldPercentInsiders': '内部人士持股比例',
    'heldPercentInstitutions': '机构持股比例',
    'bookValue': '账面价值',
    'priceToBook': '市净率(P/B)',
    'lastFiscalYearEnd': '上一财年结束日',
    'nextFiscalYearEnd': '下一财年结束日',
    'mostRecentQuarter': '最近季度',
    'earningsQuarterlyGrowth': '季度盈利增长',
    'netIncomeToCommon': '归属普通股净收入(旧)',
    'trailingEps': '追踪每股收益',
    'forwardEps': '远期每股收益',
    'lastSplitFactor': '上次拆股因子',
    'lastSplitDate': '上次拆股日期',
    'enterpriseToRevenue': '企业价值/营收',
    'enterpriseToEbitda': '企业价值/EBITDA',
    '52WeekChange': '52周变化',
    'SandP52WeekChange': '标普500 52周变化',
    'lastDividendValue': '上一股息值',
    'lastDividendDate': '上一股息日',
    'quoteType': '报价类型',
    'currentPrice': '当前价格',
    'targetHighPrice': '最高目标价',
    'targetLowPrice': '最低目标价',
    'targetMeanPrice': '平均目标价',
    'targetMedianPrice': '目标价中位数',
    'recommendationMean': '平均建议',
    'recommendationKey': '分析师建议',
    'numberOfAnalystOpinions': '分析师评级数',
    'totalCash': '总现金(Info)',
    'totalCashPerShare': '每股总现金',
    'totalDebt': '总债务(Info)',
    'totalRevenue': '总营收(Info)',
    'ebitda': 'EBITDA(Info)',
    'quickRatio': '速动比率',
    'currentRatio': '流动比率',
    'debtToEquity': '债转股',
    'revenuePerShare': '每股营收',
    'returnOnAssets': '资产回报率',
    'returnOnEquity': '股东权益回报率',
    'grossProfits': '毛利润(Info)',
    'freeCashflow': '自由现金流(Info)',
    'operatingCashflow': '经营现金流(Info)',
    'earningsGrowth': '盈利增长',
    'revenueGrowth': '营收增长',
    'grossMargins': '毛利率',
    'ebitdaMargins': 'EBITDA利润率',
    'operatingMargins': '营业利润率',
    'financialCurrency': '财务货币',
    'symbol': '代码',
    'language': '语言',
    'region': '地区',
    'exchange': '交易所',
    'shortName': '公司简称',
    'longName': '公司全称',
    'marketState': '市场状态',
    'regularMarketChangePercent': '正常市变化(%)',
    'regularMarketPrice': '正常市价格',
    'regularMarketTime': '正常市时间',
    'trailingPegRatio': '追踪市盈增长率',
    
    # --- V12/V21 新增 ---
    'quantScore': 'V21量化评分(越低越好)'
}

# 'recommendations' 数据的列标题
REC_COL_TRANSLATIONS = {
    'period': '周期',
    'strongBuy': '强烈买入',
    'buy': '买入',
    'hold': '持有',
    'sell': '卖出',
    'strongSell': '强烈卖出',
    'Firm': '评级机构',
    'To Grade': '最新评级',
    'From Grade': '原评级',
    'Action': '评级行动',
}

# 'financials', 'balance_sheet', 'cashflow' 数据的行索引
FINANCIALS_ROW_TRANSLATIONS = {
    # 利润表
    'Total Revenue': '总营收(财报)',
    'Cost Of Revenue': '营收成本',
    'Gross Profit': '毛利润',
    'Operating Income': '营业利润',
    'Net Income': '净利润',
    'Ebitda': 'EBITDA(财报)',
    'EBIT': 'EBIT(息税前利润)',
    'Total Operating Expenses': '总运营费用',
    'Operating Expense': '运营费用',
    'Research And Development': '研发费用',
    'Selling General And Administration': '销售、一般和管理费用',
    'Interest Expense': '利息支出',
    'Interest Income': '利息收入',
    'Income Before Tax': '税前利润',
    'Pretax Income': '税前利润',
    'Income Tax Expense': '所得税支出',
    'Normalized EBITDA': '标准化EBITDA',
    'Normalized Income': '标准化收入',
    'Net Income From Continuing Operation Net Minority Interest': '持续经营净利润(不含少数股东权益)',
    'Net Income Continuous Operations': '持续经营净利润',
    'Net Income Common Stockholders': '归母净利润',
    'Basic EPS': '基本每股收益',
    'Diluted EPS': '稀释每股收益',
    'Basic Average Shares': '基本平均股数',
    'Diluted Average Shares': '稀释平均股数',
    'Total Operating Income As Reported': '报告营业总收入',
    
    # 资产负债表
    'Total Assets': '总资产',
    'Total Non Current Assets': '非流动资产合计',
    'Goodwill And Other Intangible Assets': '商誉及其他无形资产',
    'Goodwill': '商誉',
    'Other Intangible Assets': '其他无形资产',
    'Net PPE': '净物业、厂房和设备',
    'Current Assets': '流动资产',
    'Inventory': '存货',
    'Receivables': '应收款',
    'Accounts Receivable': '应收账款',
    'Cash Cash Equivalents And Short Term Investments': '现金、现金等价物和短期投资',
    'Cash And Cash Equivalents': '现金及现金等价物',
    'Total Liab': '总负债',
    'Total Liabilities Net Minority Interest': '总负债(不含少数股东权益)',
    'Total Non Current Liabilities Net Minority Interest': '非流动负债合计(不含少数股东权益)',
    'Long Term Debt And Capital Lease Obligation': '长期债务和资本租赁负债',
    'Long Term Debt': '长期债务',
    'Current Liabilities': '流动负债',
    'Current Debt And Capital Lease Obligation': '流动债务和资本租赁负债',
    'Current Debt': '流动债务',
    'Payables': '应付账款',
    'Accounts Payable': '应付账款',
    'Net Debt': '净债务',
    'Total Debt': '总债务',
    'Tangible Book Value': '有形账面价值',
    'Invested Capital': '投入资本',
    'Working Capital': '营运资本',
    'Net Tangible Assets': '有形资产净值',
    'Common Stock Equity': '普通股股权',
    'Total Capitalization': '总资本',
    'Total Equity Gross Minority Interest': '总权益(含少数股东权益)',
    'Minority Interest': '少数股东权益',
    'Stockholders Equity': '股东权益合计',
    'Retained Earnings': '留存收益',
    'Treasury Stock': '库藏股',

    # 现金流量表
    'Free Cash Flow': '自由现金流',
    'Repurchase Of Capital Stock': '股票回购',
    'Repayment Of Debt': '偿还债务',
    'Issuance Of Debt': '发行债务',
    'Capital Expenditure': '资本支出',
    'End Cash Position': '期末现金头寸',
    'Beginning Cash Position': '期初现金头V', 
    'Effect Of Exchange Rate Changes': '汇率变动影响',
    'Changes In Cash': '现金变动',
    'Financing Cash Flow': '融资活动现金流',
    'Cash Dividends Paid': '支付现金股利',
    'Investing Cash Flow': '投资活动现金流',
    'Purchase Of PPE': '购买物业、厂房和设备',
    'Operating Cash Flow': '经营活动现金流',
    'Change In Working Capital': '营运资本变动',
    'Stock Based Compensation': '股权激励费用',
    'Depreciation And Amortization': '折旧和摊销',
    'Net Income From Continuing Operations': '持续经营净收入',
}

ALL_TRANSLATIONS = {
    **INFO_COL_TRANSLATIONS,
    **REC_COL_TRANSLATIONS,
    **FINANCIALS_ROW_TRANSLATIONS
}

# --- V21 新增: 加权排名打分函数 ---
def calculate_ranks_and_score_v21(df):
    """
    V21 (Weighted Sum-of-Ranks) 模型:
    采用加权排名机制，对不同类别的指标赋予不同权重。
    分数越低越好 (Low Score = Better Rank)。
    """
    print("开始进行 V21 (加权排名优化版) 打分...")
    
    # 辅助函数：安全地将列转换为数字，非数字变为 NaN
    def safe_to_numeric(column):
        return pd.to_numeric(column, errors='coerce')

    # 1. 强制转换数据类型
    # 估值类
    df['trailingPE'] = safe_to_numeric(df.get('trailingPE'))
    df['forwardPE'] = safe_to_numeric(df.get('forwardPE'))
    df['priceToBook'] = safe_to_numeric(df.get('priceToBook'))
    df['priceToSalesTrailing12Months'] = safe_to_numeric(df.get('priceToSalesTrailing12Months'))
    
    # 盈利/成长类
    df['returnOnEquity'] = safe_to_numeric(df.get('returnOnEquity'))
    df['profitMargins'] = safe_to_numeric(df.get('profitMargins'))
    df['operatingMargins'] = safe_to_numeric(df.get('operatingMargins'))
    df['revenueGrowth'] = safe_to_numeric(df.get('revenueGrowth'))
    df['earningsGrowth'] = safe_to_numeric(df.get('earningsGrowth'))
    
    # 财务健康/股息类
    df['debtToEquity'] = safe_to_numeric(df.get('debtToEquity'))
    df['currentRatio'] = safe_to_numeric(df.get('currentRatio'))
    df['quickRatio'] = safe_to_numeric(df.get('quickRatio'))
    df['dividendYield'] = safe_to_numeric(df.get('dividendYield'))
    df['payoutRatio'] = safe_to_numeric(df.get('payoutRatio'))
    
    # 市场情绪
    df['targetMeanPrice'] = safe_to_numeric(df.get('targetMeanPrice'))
    df['currentPrice'] = safe_to_numeric(df.get('currentPrice'))
    df['heldPercentInstitutions'] = safe_to_numeric(df.get('heldPercentInstitutions'))

    # --- V21 特殊处理: 优化指标逻辑 ---

    # 1. 智能 PE 处理 (Smart PE): 
    # 问题: 负数 PE (亏损) 在升序排名时会排在正数 PE (盈利) 之前 (因为 -10 < 5)。
    # 解决: 将 PE <= 0 的设为一个极大的惩罚值 (例如 9999)，让其排名垫底。
    df['smart_pe'] = df['trailingPE'].apply(lambda x: x if x > 0 else 9999)
    df['smart_fwd_pe'] = df['forwardPE'].apply(lambda x: x if x > 0 else 9999)

    # 2. PEG 估算 (PE / Growth): 
    # 如果没有现成数据，手动计算。寻找 0.5 - 1.5 之间的黄金区间。
    # 这里我们简化为: PEG 越低越好 (但在正数范围内)。
    df['calc_peg'] = df['smart_pe'] / (df['earningsGrowth'] * 100) # Growth通常是小数 0.2 -> 20
    df['calc_peg'] = df['calc_peg'].apply(lambda x: x if x > 0 else 9999) # 同样处理负值

    # 3. 派息比率距离优化:
    # 目标是 35% - 45% (0.35-0.45) 为最佳。
    df['payout_score'] = (df['payoutRatio'] - 0.4).abs()

    # 4. 目标价上涨空间:
    df['upside_potential'] = (df['targetMeanPrice'] - df['currentPrice']) / df['currentPrice']
    
    # --- 排名计算 (Rank Calculation) ---
    # pct=True: 将排名转化为百分比 (0.0 - 1.0)，方便加权计算。
    ranks = {}

    # === A. 估值权重 (Valuation) - 权重: 高 (1.5) ===
    # 越小越好
    ranks['pe_rank'] = df['smart_pe'].rank(ascending=True, pct=True, na_option='bottom')
    ranks['fwd_pe_rank'] = df['smart_fwd_pe'].rank(ascending=True, pct=True, na_option='bottom')
    ranks['pb_rank'] = df['priceToBook'].rank(ascending=True, pct=True, na_option='bottom')
    ranks['ps_rank'] = df['priceToSalesTrailing12Months'].rank(ascending=True, pct=True, na_option='bottom')

    # === B. 盈利能力 (Profitability) - 权重: 高 (1.5) ===
    # 越大越好
    ranks['roe_rank'] = df['returnOnEquity'].rank(ascending=False, pct=True, na_option='bottom')
    ranks['net_margin_rank'] = df['profitMargins'].rank(ascending=False, pct=True, na_option='bottom')
    ranks['op_margin_rank'] = df['operatingMargins'].rank(ascending=False, pct=True, na_option='bottom')

    # === C. 成长性 (Growth) - 权重: 中 (1.2) ===
    # 越大越好 (PEG例外)
    ranks['rev_growth_rank'] = df['revenueGrowth'].rank(ascending=False, pct=True, na_option='bottom')
    ranks['earn_growth_rank'] = df['earningsGrowth'].rank(ascending=False, pct=True, na_option='bottom')
    ranks['peg_rank'] = df['calc_peg'].rank(ascending=True, pct=True, na_option='bottom') # PEG越小越好

    # === D. 财务健康与股息 (Health & Yield) - 权重: 标准 (1.0) ===
    ranks['debt_equity_rank'] = df['debtToEquity'].rank(ascending=True, pct=True, na_option='bottom') # 越小越好
    ranks['current_ratio_rank'] = df['currentRatio'].rank(ascending=False, pct=True, na_option='bottom') # 越大越好
    ranks['div_yield_rank'] = df['dividendYield'].rank(ascending=False, pct=True, na_option='bottom') # 越高越好
    ranks['payout_rank'] = df['payout_score'].rank(ascending=True, pct=True, na_option='bottom') # 越接近0.4越好

    # === E. 市场情绪 (Sentiment) - 权重: 低 (0.8) ===
    ranks['inst_hold_rank'] = df['heldPercentInstitutions'].rank(ascending=False, pct=True, na_option='bottom')
    ranks['upside_rank'] = df['upside_potential'].rank(ascending=False, pct=True, na_option='bottom')

    # --- 综合加权打分 (Weighted Scoring) ---
    # 分数 = sum(rank_percentile * weight) * 100
    # 结果是一个 0 - 1400 左右的分数，越低越好
    
    total_score = (
        # A. Valuation (x 1.5)
        (ranks['pe_rank'] * 1.5) +
        (ranks['fwd_pe_rank'] * 1.5) +
        (ranks['pb_rank'] * 1.5) +
        (ranks['ps_rank'] * 1.2) + # PS稍微降低权重
        
        # B. Profitability (x 1.5)
        (ranks['roe_rank'] * 1.5) +
        (ranks['net_margin_rank'] * 1.5) +
        (ranks['op_margin_rank'] * 1.2) +
        
        # C. Growth (x 1.2)
        (ranks['rev_growth_rank'] * 1.2) +
        (ranks['earn_growth_rank'] * 1.2) +
        (ranks['peg_rank'] * 1.2) +
        
        # D. Health (x 1.0)
        (ranks['debt_equity_rank'] * 1.0) +
        (ranks['current_ratio_rank'] * 1.0) +
        (ranks['div_yield_rank'] * 1.0) +
        (ranks['payout_rank'] * 1.0) +
        
        # E. Sentiment (x 0.8)
        (ranks['inst_hold_rank'] * 0.8) +
        (ranks['upside_rank'] * 0.8)
    )

    # 归一化并取整 (Scale to 0-100 for easier reading, optional, here we just keep sum)
    # 为了保持和之前逻辑一致 (越低越好)，直接保存加权和
    df['quantScore'] = (total_score * 10).fillna(9999).astype('int')
    
    print("V21 排名打分计算完成。")
    return df

# --- 下面是爬虫核心逻辑 (保持不变，只是调用了新的打分函数) ---

def process_ticker(ticker_symbol):
    """为单个股票代码获取所有数据。"""
    combined_data = {'symbol': ticker_symbol}
    
    try:
        ticker = yf.Ticker(ticker_symbol)
        
        # --- 智能重试 ---
        MAX_RETRIES = 5
        RETRY_DELAY = 60 # 429限速错误的基础等待时间
        
        info = None
        for attempt in range(MAX_RETRIES):
            try:
                info = ticker.info
                break 
            except Exception as e:
                error_str = str(e)
                if "Too Many Requests" in error_str or "Rate limited" in error_str:
                    wait_time = RETRY_DELAY * (attempt + 1)
                    # print(f"  [限速] {ticker_symbol} 等待 {wait_time} 秒...")
                    time.sleep(wait_time)
                elif "404" in error_str or "Not Found" in error_str:
                    return None
                else:
                    if attempt == MAX_RETRIES - 1:
                        return None
                    time.sleep(5)

        if not info:
            return None
            
        # 简单校验数据有效性
        if not info.get('marketCap'): 
            return None
        
        combined_data.update(info)
            
        # 获取 Financials (只保留最近一个财年)
        try:
            fin = ticker.financials
            if not fin.empty:
                latest_fin = fin.iloc[:, 0] 
                if isinstance(latest_fin.name, pd.Timestamp):
                    latest_fin.name = latest_fin.name.tz_localize(None)
                combined_data.update(latest_fin)
        except Exception:
            pass 
            
        # 获取 Balance Sheet
        try:
            bal = ticker.balance_sheet
            if not bal.empty:
                latest_bal = bal.iloc[:, 0] 
                if isinstance(latest_bal.name, pd.Timestamp):
                    latest_bal.name = latest_bal.name.tz_localize(None)
                combined_data.update(latest_bal)
        except Exception:
            pass 

        sleep_time = random.uniform(1.0, 2.0)
        time.sleep(sleep_time)

        return combined_data
        
    except Exception:
        return None

i = 0

def get_hk_stock_info_combined(tickers, output_filename="hk_stocks_info_v21.xlsx"):
    global i
    all_combined_data = []
    total_tickers = len(tickers)
    MAX_WORKERS = 8 

    print(f"开始并发获取 {total_tickers} 只股票 (V21 优化版)...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {executor.submit(process_ticker, ticker): ticker for ticker in tickers}
        
        completed_count = 0
        
        for future in concurrent.futures.as_completed(future_to_ticker):
            completed_count += 1
            i = completed_count 
            
            result = future.result()
            if result:
                all_combined_data.append(result)

            if completed_count % 50 == 0:
                print(f"进度: {completed_count}/{total_tickers} (成功: {len(all_combined_data)})")

    print("\n数据获取完毕，正在计算 V21 量化分数...")

    try:
        if all_combined_data:
            df_raw = pd.DataFrame(all_combined_data)
            
            # --- 核心修改：调用 V21 打分函数 ---
            try:
                df_raw_scored = calculate_ranks_and_score_v21(df_raw)
            except Exception as e:
                print(f"[错误] V21 打分失败: {e}")
                df_raw['quantScore'] = 0
                df_raw_scored = df_raw
            # --------------------------------

            df_combined = df_raw_scored.rename(columns=ALL_TRANSLATIONS)
            
            # 调整列顺序，把分数放前面
            translated_symbol_col = ALL_TRANSLATIONS.get('symbol', '代码')
            translated_score_col = ALL_TRANSLATIONS.get('quantScore', 'V21量化评分(越低越好)')

            cols = list(df_combined.columns)
            if translated_score_col in cols:
                cols.insert(0, cols.pop(cols.index(translated_score_col)))
            if translated_symbol_col in cols:
                cols.insert(0, cols.pop(cols.index(translated_symbol_col)))
                
            df_combined = df_combined[cols]
            
            # 根据分数排序 (V21 越低越好)
            if translated_score_col in df_combined.columns:
                df_combined = df_combined.sort_values(by=translated_score_col, ascending=True)

            df_combined.to_excel(output_filename, sheet_name='V21量化分析', index=False)
            print(f"\n成功! 结果已保存至 {output_filename}")
        else:
            print("未获取到有效数据。")

    except Exception as e:
        print(f"保存文件出错: {e}")

def generate_ticker_list(start=1, end=5000, suffix=".HK"):
    print(f"生成代码列表: {start:04d}{suffix} - {end:04d}{suffix}")
    return [f"{i:04d}{suffix}" for i in range(start, end + 1)]

if __name__ == "__main__":
    # 示例：抓取前 100 只股票测试效果
    hk_ticker_list = generate_ticker_list(1, 4000) 
    
    if len(sys.argv) > 1:
        output_filename = sys.argv[1]
    else:
        timestamp = time.strftime("%Y%m%d_%H%M", time.localtime())
        output_filename = f"HK_Stocks_V21_{timestamp}.xlsx"
    
    get_hk_stock_info_combined(hk_ticker_list, output_filename=output_filename)
