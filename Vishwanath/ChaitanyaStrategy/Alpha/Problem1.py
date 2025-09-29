import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
import warnings
warnings.filterwarnings('ignore')

plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

tickers = {
    'GC=F': 'Gold',
    'SI=F': 'Silver', 
    'JPY=X': 'USDJPY',
    '^SPX': 'SPX',
    '^VIX': 'VIX'
}

start_date = '2016-01-01'
end_date = '2025-08-31'

def fetch_data(tickers, start_date, end_date):
    data = {}
    for ticker, name in tickers.items():
        print(f"Fetching {name} ({ticker})...")
        df = yf.download(ticker, start=start_date, end=end_date)
        close_col = 'Adj Close' if 'Adj Close' in df.columns else 'Close'
        data[name] = df[close_col]
    
    combined_df = pd.concat(data, axis=1)
    combined_df.columns = combined_df.columns.get_level_values(0)
    return combined_df

def generate_signals(df):
    signals = pd.DataFrame(index=df.index)
    signals['GS_Mean_Reversion'] = np.where(
        df['GS_Z_Score'] > 1.5, -1, np.where(df['GS_Z_Score'] < -1.5, 1, 0)
    )
    signals['Gold_Momentum'] = np.where(
        (df['Gold_MA_20'] > df['Gold_MA_50']) & (df['Gold_Momentum_10'] > 0), 1, 0
    )
    signals['Silver_Momentum'] = np.where(
        (df['Silver_MA_20'] > df['Silver_MA_50']) & (df['Silver_Momentum_10'] > 0), 1, 0
    )
    signals['Low_Volatility'] = np.where(df['VIX'] < df['VIX_MA_20'], 1, 0)
    signals['Bull_Market'] = np.where(
        (df['SPX'] > df['SPX'].rolling(50).mean()) & (df['VIX'] < 20), 1, 0
    )
    
    return signals

def create_ml_features(df):
    features = pd.DataFrame(index=df.index)
    features['Target'] = np.where(df['Gold'].shift(-5) > df['Gold'], 1, 0)
    
    feature_columns = [
        'GS_Z_Score', 'Gold_RSI_14', 'Silver_RSI_14', 'VIX_Change',
        'SPX_Volatility_20', 'Gold_Silver_Corr_20', 'Gold_SPX_Corr_20',
        'USDJPY_Trend', 'Gold_MA_20', 'Gold_MA_50'
    ]
    
    for col in feature_columns:
        features[col] = df[col]
    
    return features.dropna()

def implement_strategy(df, signals, ml_signal):
    strategy = pd.DataFrame(index=df.index)    
    strategy['Gold_Position'] = 0
    strategy['Silver_Position'] = 0
    strategy.loc[signals['GS_Mean_Reversion'] == 1, 'Gold_Position'] = 1
    strategy.loc[signals['GS_Mean_Reversion'] == 1, 'Silver_Position'] = -1
    strategy.loc[signals['GS_Mean_Reversion'] == -1, 'Gold_Position'] = -1
    strategy.loc[signals['GS_Mean_Reversion'] == -1, 'Silver_Position'] = 1
    aligned_ml_signal = ml_signal.reindex(strategy.index, fill_value=0)
    strategy.loc[aligned_ml_signal == 1, 'Gold_Position'] += 0.5
    strategy.loc[aligned_ml_signal == 0, 'Gold_Position'] -= 0.5    
    strategy.loc[signals['Low_Volatility'] == 0, ['Gold_Position', 'Silver_Position']] *= 0.5
    strategy['Gold_Position'] = np.clip(strategy['Gold_Position'], -1, 1)
    strategy['Silver_Position'] = np.clip(strategy['Silver_Position'], -1, 1)
    
    return strategy

def backtest_strategy(df, strategy):
    results = pd.DataFrame(index=df.index)
    results['Gold_Returns'] = df['Gold'].pct_change()
    results['Silver_Returns'] = df['Silver'].pct_change()
    results['Strategy_Returns'] = (
        strategy['Gold_Position'].shift(1) * results['Gold_Returns'] +
        strategy['Silver_Position'].shift(1) * results['Silver_Returns']
    )    
    results['Cumulative_Strategy'] = (1 + results['Strategy_Returns']).cumprod()
    results['Cumulative_Gold'] = (1 + results['Gold_Returns']).cumprod()
    results['Cumulative_Silver'] = (1 + results['Silver_Returns']).cumprod()    
    results['Benchmark_Returns'] = 0.5 * results['Gold_Returns'] + 0.5 * results['Silver_Returns']
    results['Cumulative_Benchmark'] = (1 + results['Benchmark_Returns']).cumprod()
    
    return results

def calculate_metrics(returns):
    metrics = {}
    annual_return = (1 + returns).prod() ** (252/len(returns)) - 1
    metrics['Annualized Return'] = annual_return    
    metrics['Annualized Volatility'] = returns.std() * np.sqrt(252)
    metrics['Sharpe Ratio'] = annual_return / metrics['Annualized Volatility'] if metrics['Annualized Volatility'] != 0 else 0    
    cumulative = (1 + returns).cumprod()
    peak = cumulative.expanding().max()
    drawdown = (cumulative - peak) / peak
    metrics['Max Drawdown'] = drawdown.min()    
    metrics['Win Rate'] = (returns > 0).mean()
    
    return metrics

df = fetch_data(tickers, start_date, end_date)
df = df.ffill().bfill()
df['Gold_Silver_Ratio'] = df['Gold'] / df['Silver']
df['GS_Ratio_MA_20'] = df['Gold_Silver_Ratio'].rolling(window=20).mean()
df['GS_Ratio_Std_20'] = df['Gold_Silver_Ratio'].rolling(window=20).std()
df['GS_Z_Score'] = (df['Gold_Silver_Ratio'] - df['GS_Ratio_MA_20']) / df['GS_Ratio_Std_20']

for asset in ['Gold', 'Silver']:
    df[f'{asset}_MA_20'] = df[asset].rolling(window=20).mean()
    df[f'{asset}_MA_50'] = df[asset].rolling(window=50).mean()
    df[f'{asset}_RSI_14'] = 100 - (100 / (1 + (df[asset].pct_change().rolling(14).mean() / 
                                            df[asset].pct_change().rolling(14).std())))
    df[f'{asset}_Momentum_10'] = df[asset].pct_change(10)

df['VIX_MA_20'] = df['VIX'].rolling(window=20).mean()
df['VIX_Change'] = df['VIX'].pct_change()
df['SPX_Volatility_20'] = df['SPX'].pct_change().rolling(20).std()
df['Gold_Silver_Corr_20'] = df['Gold'].pct_change().rolling(20).corr(df['Silver'].pct_change())
df['Gold_SPX_Corr_20'] = df['Gold'].pct_change().rolling(20).corr(df['SPX'].pct_change())
df['USDJPY_MA_20'] = df['USDJPY'].rolling(window=20).mean()
df['USDJPY_Trend'] = np.where(df['USDJPY'] > df['USDJPY_MA_20'], 1, -1)
signals = generate_signals(df)
ml_features = create_ml_features(df)
X = ml_features.drop('Target', axis=1)
y = ml_features['Target']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
rf_model.fit(X_train, y_train)
y_pred = rf_model.predict(X_test)
ml_features['ML_Signal'] = rf_model.predict(X)

print(f"ML Model Accuracy: {accuracy_score(y_test, y_pred):.3f}")
print(classification_report(y_test, y_pred))
trading_strategy = implement_strategy(df, signals, ml_features['ML_Signal'])
backtest_results = backtest_strategy(df, trading_strategy)
strategy_metrics = calculate_metrics(backtest_results['Strategy_Returns'].dropna())
benchmark_metrics = calculate_metrics(backtest_results['Benchmark_Returns'].dropna())

plt.figure(figsize=(15, 8))
plt.subplot(1, 2, 1)
plt.plot(backtest_results['Cumulative_Strategy'], label='Trading Strategy', linewidth=2)
plt.plot(backtest_results['Cumulative_Benchmark'], label='Benchmark (50/50)', linewidth=2)
plt.plot(backtest_results['Cumulative_Gold'], label='Gold', alpha=0.7)
plt.plot(backtest_results['Cumulative_Silver'], label='Silver', alpha=0.7)
plt.title('Cumulative Returns')
plt.legend()
plt.grid(True)

plt.subplot(1, 2, 2)
plt.plot(df['Gold_Silver_Ratio'], label='Gold/Silver Ratio', color='blue')
plt.plot(df['GS_Ratio_MA_20'], label='20D MA', color='red', linestyle='--')
plt.fill_between(df.index, 
                df['GS_Ratio_MA_20'] - df['GS_Ratio_Std_20'], 
                df['GS_Ratio_MA_20'] + df['GS_Ratio_Std_20'], 
                alpha=0.2, color='red')
plt.title('Gold-Silver Ratio with Bollinger Bands')
plt.legend()
plt.grid(True)

plt.tight_layout()
plt.show()

print(f"\nStrategy Performance Metrics:")
for metric, value in strategy_metrics.items():
    print(f"{metric}: {value:.4f}")

print(f"\nBenchmark Performance Metrics:")
for metric, value in benchmark_metrics.items():
    print(f"{metric}: {value:.4f}")

print(f"\nFinal Strategy Value: ${backtest_results['Cumulative_Strategy'].iloc[-1]:.2f}")
print(f"Final Benchmark Value: ${backtest_results['Cumulative_Benchmark'].iloc[-1]:.2f}")
print(f"Outperformance: {(backtest_results['Cumulative_Strategy'].iloc[-1] / backtest_results['Cumulative_Benchmark'].iloc[-1] - 1) * 100:.2f}%")

feature_importance = pd.DataFrame({
    'Feature': X.columns,
    'Importance': rf_model.feature_importances_
}).sort_values('Importance', ascending=False)

print(f"\nTop ML Features:")
print(feature_importance.head(10))