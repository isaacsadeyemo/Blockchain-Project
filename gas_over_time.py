import pandas as pd
import os
import matplotlib.pyplot as plt
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
from itertools import islice

CHUNK_SIZE = 100000
CPU_COUNT = os.cpu_count()

def process_chunk(chunk):
    unique_txs = chunk.copy()
    unique_txs['date'] = pd.to_datetime(unique_txs['signed_at']).dt.date
    gas_spent_agg = unique_txs.groupby('date')['tx_gas_spent'].sum()
    gas_price_agg = unique_txs.groupby('date')['tx_gas_price'].mean()
    return gas_spent_agg, gas_price_agg

def split_dataframe(df, chunk_size=CHUNK_SIZE): 
    num_chunks = len(df) // chunk_size + 1
    return (df[i*chunk_size:(i+1)*chunk_size] for i in range(num_chunks))

def annotate_min_max(dates, values, ax, color):
    min_val = min(values)
    max_val = max(values)
    min_idx = values.index(min_val)
    max_idx = values.index(max_val)
    ax.annotate(f'Min: {min_val}\n{dates[min_idx]}', xy=(dates[min_idx], min_val), 
                xytext=(-100, 30), textcoords='offset points', color=color, 
                arrowprops=dict(arrowstyle='->', color=color))
    ax.annotate(f'Max: {max_val}\n{dates[max_idx]}', xy=(dates[max_idx], max_val), 
                xytext=(-100, -30), textcoords='offset points', color=color, 
                arrowprops=dict(arrowstyle='->', color=color))

if __name__ == '__main__':
    print("Starting the program...")

    df = pd.read_csv(r"C:\Users\Georgino\Desktop\FNCE-559-Project-2\group4.csv")

    df = df.drop_duplicates(subset='tx_hash', keep='first')

    chunks = split_dataframe(df, CHUNK_SIZE)

    daily_gas_spent = defaultdict(int)
    daily_gas_price = defaultdict(float)
    daily_gas_price_counts = defaultdict(int)

    with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
        for chunk_number, results in enumerate(executor.map(process_chunk, chunks)):
            gas_spent_agg, gas_price_agg = results
            
            for date, value in gas_spent_agg.items():
                daily_gas_spent[date] += value

            for date, price in gas_price_agg.items():
                daily_gas_price[date] += price
                daily_gas_price_counts[date] += 1

            if chunk_number % 10 == 0:
                print(f"Processed {chunk_number * CHUNK_SIZE:,} rows...")

    for date in daily_gas_price:
        daily_gas_price[date] /= daily_gas_price_counts[date]

    sorted_dates = sorted(daily_gas_spent.keys())
    
    sorted_gas_spent = [daily_gas_spent[date] for date in sorted_dates]
    plt.figure(figsize=(12, 6))
    plt.plot(sorted_dates, sorted_gas_spent, label='Gas Spent', color='blue')
    annotate_min_max(sorted_dates, sorted_gas_spent, plt.gca(), 'blue')
    plt.title('Total Gas Spent Over Time')
    plt.xlabel('Date')
    plt.ylabel('Gas Spent')
    plt.tight_layout()
    plt.legend()
    filename = 'gas_spent_trend.png'
    plt.savefig(filename, dpi=300)
    plt.close()

    sorted_gas_price = [daily_gas_price[date] for date in sorted_dates]
    plt.figure(figsize=(12, 6))
    plt.plot(sorted_dates, sorted_gas_price, label='Gas Price', color='green')
    annotate_min_max(sorted_dates, sorted_gas_price, plt.gca(), 'green')
    plt.title('Average Daily Gas Price Over Time')
    plt.xlabel('Date')
    plt.ylabel('Gas Price')
    plt.tight_layout()
    plt.legend()
    filename = 'gas_price_trend.png'
    plt.savefig(filename, dpi=300)
    plt.close()

    print("Visualizations saved!")
