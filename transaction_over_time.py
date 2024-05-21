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
    return unique_txs.groupby('date').size()

def split_dataframe(df, chunk_size=CHUNK_SIZE): 
    num_chunks = len(df) // chunk_size + 1
    return (df[i*chunk_size:(i+1)*chunk_size] for i in range(num_chunks))

if __name__ == '__main__':
    print("Starting the program...")

    df = pd.read_csv(r"C:\Users\Georgino\Desktop\FNCE-559-Project-2\group4.csv")

    df = df.drop_duplicates(subset='tx_hash', keep='first')

    chunks = split_dataframe(df, CHUNK_SIZE)

    daily_transaction_counts = defaultdict(int)

    with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
        for chunk_number, date_counts in enumerate(executor.map(process_chunk, chunks)):
            for date, count in date_counts.items():
                daily_transaction_counts[date] += count
            
            if chunk_number % 10 == 0:
                print(f"Processed {chunk_number * CHUNK_SIZE:,} rows...")

    sorted_dates = sorted(daily_transaction_counts.keys())
    sorted_counts = [daily_transaction_counts[date] for date in sorted_dates]

    plt.figure(figsize=(12, 6))
    plt.plot(sorted_dates, sorted_counts, label='Number of Transactions', color='blue')
    plt.title('Transaction Trend Over Time')
    plt.xlabel('Date')
    plt.ylabel('Number of Transactions')
    plt.tight_layout()

    max_count = max(sorted_counts)
    min_count = min(sorted_counts)
    max_date = sorted_dates[sorted_counts.index(max_count)]
    min_date = sorted_dates[sorted_counts.index(min_count)]

    plt.annotate(f'Highest on {max_date}: {max_count}', xy=(max_date, max_count), xytext=(max_date, max_count * 1.1),
                 arrowprops=dict(facecolor='red', shrink=0.05))
    plt.annotate(f'Lowest on {min_date}: {min_count}', xy=(min_date, min_count), xytext=(min_date, min_count * 1.1),
                 arrowprops=dict(facecolor='green', shrink=0.05))

    plt.legend()
    filename = 'transaction_trend.png'
    plt.savefig(filename, dpi=300)
    plt.close()

    print("Visualizations saved!")
