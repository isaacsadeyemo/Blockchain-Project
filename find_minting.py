import pandas as pd

FILE_PATH = r"C:\Users\Georgino\Desktop\FNCE-559-Project-2\group4.csv"
OUTPUT_PATH = r"C:\Users\Georgino\Desktop\FNCE-559-Project-2\rows_with_zero_topic1.csv"

def main():
    df = pd.read_csv(FILE_PATH)

    zero_topic1_rows = df[df['topic1'] == '0' * len(df['topic1'].iloc[0])]

    if not zero_topic1_rows.empty:
        print(f"Found {len(zero_topic1_rows)} rows where 'topic1' is all zeros.")
        zero_topic1_rows.to_csv(OUTPUT_PATH, index=False)
        print(f"Saved these rows to: {OUTPUT_PATH}")
    else:
        print("No rows found where 'topic1' is all zeros.")

if __name__ == '__main__':
    main()
