import yfinance as yf
import pandas as pd
import time
import concurrent.futures
import random
import sys  # V13.1 修复: 导入 sys 模块以读取参数

# --- 中文翻译字典 (V12 修订版) ---

# 'info' 数据的列标题 (Column Headers)
INFO_COL_TRANSLATIONS = {
    # V15 修复: 修正了此处的缩进错误
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
    
    # --- V12 新增 ---
    'quantScore': '量化分数'
}

# 'recommendations' 数据的列标题 (Column Headers)
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

# 'financials', 'balance_sheet', 'cashflow' 数据的行索引 (Row Index)
FINANCIALS_ROW_TRANSLATIONS = {
    # 利润表 (Income Statement)
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
    
    # 资产负债表 (Balance Sheet)
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

    # 现金流量表 (Cash Flow)
    'Free Cash Flow': '自由现金流',
    'Repurchase Of Capital Stock': '股票回购',
    'Repayment Of Debt': '偿还债务',
    'Issuance Of Debt': '发行债务',
    'Capital Expenditure': '资本支出',
    'End Cash Position': '期末现金头寸',
    'Beginning Cash Position': '期初现金头V', # 修复了拼写错误
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

# --- 合并所有翻译字典 ---
ALL_TRANSLATIONS = {
    **INFO_COL_TRANSLATIONS,
    **REC_COL_TRANSLATIONS,
    **FINANCIALS_ROW_TRANSLATIONS
}
# --- 翻译字典结束 ---

# --- V12 新增：量化打分函数 ---
def calculate_quant_score(data):
    """
    根据传入的股票数据字典，计算一个量化分数。
    这是一个示例模型，您可以根据自己的策略修改这里的“10个数值”和逻辑。
    """
    score = 0
    
    # --- 1. 估值指标 (越低越好) ---
    pe = data.get('trailingPE')
    pb = data.get('priceToBook')
    
    # 市盈率 (PE)
    if pe is not None and 0 < pe < 20:
        score += 1
    if pe is not None and 0 < pe < 12: # 低于12分，再加1分
        score += 1
        
    # 市净率 (PB)
    if pb is not None and 0 < pb < 2.0:
        score += 1
    if pb is not None and 0 < pb < 1.2: # 低于1.2分，再加1分
        score += 1

    # --- 2. 盈利能力指标 (越高越好) ---
    roe = data.get('returnOnEquity')
    profit_margin = data.get('profitMargins') # 注意：yfinance 的值是 0.1 这种
    
    # 净资产收益率 (ROE)
    if roe is not None and roe > 0.10: # 大于 10%
        score += 1
    if roe is not None and roe > 0.15: # 大于 15%
        score += 2 # 优质指标，多加分

    # 净利润率
    if profit_margin is not None and profit_margin > 0.10: # 大于 10%
        score += 1
        
    # --- 3. 财务健康 (债务越低越好) ---
    de = data.get('debtToEquity')
    
    # 债转股 (Debt/Equity)
    if de is not None and 0 < de < 100: # yfinance 的 D/E 单位是百分比
        score += 1 # 债务低于净资产
        
    # --- 4. 股息 (越高越好) ---
    dy = data.get('dividendYield') # yfinance 的值是 0.05 这种
    
    if dy is not None and dy > 0.03: # 股息率大于 3%
        score += 1
    # V12.1 修复: 股息率 > 5% 才加分 (避免重复加分)
    if dy is not None and dy > 0.05: # 股息率大于 5%
        score += 1 # 总分 2 分
        
    # 最终总分 (满分 11 分, V12.1)
    # 满分 10 分 (V12)
    return score
# --- V12 结束 ---


def process_ticker(ticker_symbol):
    """
    为单个股票代码获取所有数据。
    (V14: 增加智能重试逻辑)
    """
    
    combined_data = {'symbol': ticker_symbol}
    
    try:
        ticker = yf.Ticker(ticker_symbol)
        
        # --- (V14 修复: 智能重试) ---
        MAX_RETRIES = 3
        RETRY_DELAY = 30 # 429限速错误的基础等待时间 (秒)
        
        info = None
        for attempt in range(MAX_RETRIES):
            try:
                info = ticker.info
                # 成功获取，跳出重试循环
                break 
                
            except Exception as e:
                error_str = str(e)
                
                if "Too Many Requests" in error_str or "Rate limited" in error_str:
                    # 这是 429 限速错误
                    wait_time = RETRY_DELAY * (attempt + 1)
                    print(f"  [限速] {ticker_symbol} 第 {attempt + 1} 次尝试失败。等待 {wait_time} 秒...")
                    time.sleep(wait_time) # 30s, 60s, 90s
                
                elif "404" in error_str or "Not Found" in error_str:
                    # 这是 404 错误，重试无效
                    if (i + 1) % 50 == 0:
                        print(f"  [跳过 404] {ticker_symbol} 无数据。")
                    return None # 立即跳出函数
                
                else:
                    # 其他网络错误 (例如 503)，短暂重试
                    if (i + 1) % 50 == 0:
                         print(f"  [跳过] 获取 {ticker_symbol} 'info' 时出错: {e}。")
                    if attempt == MAX_RETRIES - 1: # 最后一次尝试失败
                        return None
                    time.sleep(5) # 短暂等待5秒

        # 检查重试循环是否成功
        if not info:
            print(f"  [失败] {ticker_symbol} 在 {MAX_RETRIES} 次尝试后 'info' 仍失败。")
            return None
        # --- (V14 修复结束) ---

        # 检查 'info' 内容是否有效
        if (not info or 
            info.get('symbol') != ticker_symbol or 
            info.get('marketCap') is None): 
            
            if (i + 1) % 50 == 0: 
                 print(f"  [跳过] 股票代码 {ticker_symbol} 无效或无数据。")
            return None 
        
        combined_data.update(info)
            
        # --- 只有 Info 成功后 (代码有效)，才执行以下操作 ---
    
        # 2. 获取 Financials (只保留最近一个财年)
        try:
            fin = ticker.financials
            if not fin.empty:
                latest_fin = fin.iloc[:, 0] 
                if isinstance(latest_fin.name, pd.Timestamp):
                    latest_fin.name = latest_fin.name.tz_localize(None)
                combined_data.update(latest_fin)
        except Exception:
            pass 
            
        # 3. 获取 Balance Sheet (只保留最近一个财年)
        try:
            bal = ticker.balance_sheet
            if not bal.empty:
                latest_bal = bal.iloc[:, 0] 
                if isinstance(latest_bal.name, pd.Timestamp):
                    latest_bal.name = latest_bal.name.tz_localize(None)
                combined_data.update(latest_bal)
        except Exception:
            pass 

        # 4. 获取 Cashflow (只保留最近一个财年)
        try:
            cf = ticker.cashflow
            if not cf.empty:
                latest_cf = cf.iloc[:, 0] 
                if isinstance(latest_cf.name, pd.Timestamp):
                    latest_cf.name = latest_cf.name.tz_localize(None)
                combined_data.update(latest_cf)
        except Exception:
            pass 
            
        # 5. 获取 Recommendations (只保留最新一条评级)
        try:
            rec = ticker.recommendations
            if not rec.empty:
                latest_rec = rec.iloc[-1] 
                if isinstance(latest_rec.name, pd.Timestamp):
                    latest_rec.name = latest_rec.name.tz_localize(None)
                combined_data.update(latest_rec)
        except Exception:
            pass 

        # --- V12 新增: 计算量化分数 ---
        # 在所有数据都抓取完毕后，统一计算分数
        try:
            score = calculate_quant_score(combined_data)
            combined_data['quantScore'] = score
        except Exception as e:
            print(f"  [警告] {ticker_symbol} 计算分数时出错: {e}")
            combined_data['quantScore'] = None
        # --- V12 结束 ---

        # (V14 修复) 增加基础延迟
        sleep_time = random.uniform(2.0, 4.0)
        time.sleep(sleep_time)

        return combined_data
        
    except Exception as e:
        # 捕获 yf.Ticker(ticker_symbol) 本身的初始化错误
        if (i + 1) % 50 == 0:
            print(f"  [重大错误] 处理 {ticker_symbol} 时出错: {e}")
        return None

# V10: 声明一个全局变量 i 以便在 process_ticker 中访问进度
i = 0

def get_hk_stock_info_combined(tickers, output_filename="hk_stocks_info_combined.xlsx"):
    """
    (V14 - 增加智能重试)
    使用多线程并发获取指定港股列表的多种信息，
    将所有获取到的列合并到 Excel 的一个工作表中。
    """
    
    global i # 声明使用全局变量 i
    
    all_combined_data = []
    total_tickers = len(tickers)
    # (V14 修复) 降低并发数
    MAX_WORKERS = 5 

    print(f"开始并发获取 {total_tickers} 只股票的合并信息 (使用 {MAX_WORKERS} 个线程)...")
    print("注意：V14版将保存所有获取到的列 (包含量化分数和智能重试)。")

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
                print(f"--- 已处理 {completed_count}/{total_tickers} (当前成功获取: {len(all_combined_data)} 只) ---")

    print("\n信息收集完毕，正在处理并写入 Excel 文件...")

    try:
        if all_combined_data:
            # V11: 直接使用 df_combined 进行后续操作
            df_combined = pd.DataFrame(all_combined_data)
            df_combined = df_combined.rename(columns=ALL_TRANSLATIONS)
            
            # --- V11: 移除了白名单筛选步骤 ---
            print(f"已获取 {len(df_combined.columns)} 列数据，将全部保存。")

            # --- V12 修改: 将 '代码' 和 '量化分数' 列移动到第一、二位 ---
            translated_symbol_col = ALL_TRANSLATIONS.get('symbol', '代码')
            translated_score_col = ALL_TRANSLATIONS.get('quantScore', '量化分数')

            cols = list(df_combined.columns)
            
            # 移动 '量化分数'
            if translated_score_col in cols:
                cols.insert(0, cols.pop(cols.index(translated_score_col)))
            
            # 移动 '代码'
            if translated_symbol_col in cols:
                cols.insert(0, cols.pop(cols.index(translated_symbol_col)))
            else:
                print("[警告] 未找到 '代码' 列。")
                
            df_combined = df_combined[cols]
            # --- V12 结束 ---
            
            # --- 保存到 Excel ---
            # V11: 保存 df_combined 并修改 sheet_name
            df_combined.to_excel(output_filename, sheet_name='全部信息', index=False)
            print(f"\n成功将全部数据保存到 {output_filename}")
        else:
            print("未能获取到任何有效股票信息。")

    except Exception as e:
        print(f"保存到 Excel 时出错: {e}")
        print("请确保已安装 'openpyxl' 库 (pip install openpyxl)")

def generate_ticker_list(start=1, end=5000, suffix=".HK"):
    """
    生成从 start 到 end 的4位数代码列表 (例如 '0001.HK')
    """
    print(f"正在生成从 {start:04d}{suffix} 到 {end:04d}{suffix} 的代码列表...")
    return [f"{i:04d}{suffix}" for i in range(start, end + 1)]

if __name__ == "__main__":
    # --- 自动生成 0001.HK 到 5000.HK 的列表 (您可以按需修改范围) ---
    hk_ticker_list = generate_ticker_list(1, 10)
    
    # --- (V13.1 修复) ---
    # 检查是否从 GitHub Actions (main.yml) 传入了文件名参数
    
    if len(sys.argv) > 1:
        # 如果传入了参数 (例如 'python script.py filename.xlsx')
        # 则使用 main.yml 传入的文件名
        output_filename = sys.argv[1]
        print(f"检测到 GitHub Actions 传入文件名: {output_filename}")
    else:
        # 如果没有传入参数 (本地运行)
        # 则使用旧方法，根据当前时间生成文件名
        print("未检测到传入文件名，本地运行模式，自动生成文件名...")
        timestamp = time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())
        output_filename = f"{timestamp}.xlsx"
    # --- (修复结束) ---
    
    get_hk_stock_info_combined(hk_ticker_list, output_filename=output_filename)
