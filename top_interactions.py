import pandas as pd
import os
import matplotlib.pyplot as plt
from concurrent.futures import ProcessPoolExecutor
from collections import Counter
from itertools import repeat, islice

CHUNK_SIZE = 100000
CPU_COUNT = os.cpu_count()

def process_chunk(chunk):
    senders = chunk['tx_sender'].tolist()
    recipients = chunk['tx_recipient'].tolist()
    interactions = list(zip(senders, recipients))
    
    return senders, recipients, interactions

def split_dataframe(df, chunk_size = CHUNK_SIZE): 
    num_chunks = len(df) // chunk_size + 1
    return (df[i*chunk_size:(i+1)*chunk_size] for i in range(num_chunks))

if __name__ == '__main__':
    print("Starting the program...")

    df = pd.read_csv("group4.csv")

    df = df.drop_duplicates(subset='tx_hash', keep='first')
    
    print(len(df.index))

    all_senders = []
    all_recipients = []
    all_interactions = []

    chunks = split_dataframe(df, CHUNK_SIZE)
    
    

    with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
        for chunk_number, (senders, recipients, interactions) in enumerate(executor.map(process_chunk, chunks)):
            all_senders.extend(senders)
            all_recipients.extend(recipients)
            all_interactions.extend(interactions)
            
            total_chunks = (len(df) // CHUNK_SIZE) + 1

            if chunk_number % 10 == 0 or chunk_number == total_chunks - 1:
                print(f"Processed {chunk_number * CHUNK_SIZE:,} rows...")

    sender_counts = Counter(all_senders)
    recipient_counts = Counter(all_recipients)
    interaction_counts = Counter(all_interactions)

    top_senders = sender_counts.most_common(10)
    top_recipients = recipient_counts.most_common(10)
    top_interactions = interaction_counts.most_common(10)

    sender_addresses, sender_frequencies = zip(*top_senders)
    plt.figure(figsize=(10, 5))
    plt.barh(sender_addresses, sender_frequencies, color='blue')
    plt.title('Top 10 Senders')
    plt.ylabel('Sender Address')
    plt.xlabel('Number of Transactions Initiated')
    plt.tight_layout()
    plt.savefig('top_senders.png', dpi=300)
    plt.close()

    recipient_addresses, recipient_frequencies = zip(*top_recipients)
    plt.figure(figsize=(10, 5))
    plt.barh(recipient_addresses, recipient_frequencies, color='green')
    plt.title('Top 10 Recipients')
    plt.ylabel('Recipient Address')
    plt.xlabel('Number of Transactions Received')
    plt.tight_layout()
    plt.savefig('top_recipients.png', dpi=300)
    plt.close()

    interaction_pairs, interaction_frequencies = zip(*top_interactions)
    interaction_labels = [f"{s} -> {r}" for s, r in interaction_pairs]
    plt.figure(figsize=(10, 5))
    plt.barh(interaction_labels, interaction_frequencies, color='red')
    plt.title('Top 10 Address Interactions')
    plt.ylabel('Sender -> Recipient')
    plt.xlabel('Number of Interactions')
    plt.tight_layout()
    plt.savefig('top_interactions.png', dpi=300)
    plt.close()

    print("Top Interactions")
    for i in sender_addresses:
        print(i)

    print("Visualizations generated successfully!")
