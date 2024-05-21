using DataFrames
using CSV
using VegaLite
using Statistics
using Dates
using PlotlyJS

data = CSV.read("C:/Users/ayesh/OneDrive/Documents/juliaproject3/group3.csv", DataFrame)
# Group by recipient and count interactions
counts = combine(groupby(data, :logemitter), nrow)

# Group by sender and count interactions

# Sort recipient counts in descending order
sorted_counts = sort(counts, :nrow, rev=true)
#sorted_counts = sort(counts, nrow)


# Get the top N interacting recipients
top_contracts = first(sorted_counts, 10)
println(top_contracts)
num_rows_to_print = 10

for row_number in 1:num_rows_to_print
    full_string = top_contracts[row_number, :nrow]
    println(full_string)
end

#*************************************************************************************
counts = combine(groupby(data, :data0), nrow)

# Group by sender and count interactions

# Sort recipient counts in descending order
sorted_counts = sort(counts, :nrow, rev=true)
#sorted_counts = sort(counts, nrow)


# Get the top N interacting recipients
top_contracts = first(sorted_counts, 10)
println(top_contracts)
num_rows_to_print = 10

for row_number in 1:num_rows_to_print
    full_string = top_contracts[row_number, :nrow]
    println(full_string)
end
#**************************************************************************************

#top topic0s in terms of "D78A"
file_path = "C:/Users/ayesh/OneDrive/Documents/juliaproject3/group3.csv"

# Check if the file exists
if isfile(file_path)
    try
        df = CSV.File(file_path) |> DataFrame
        filtered_df = filter(row -> startswith(row.topic0x, "D78A"), df)
        
        if nrow(filtered_df) > 0
            # Replace 'filtered_output.csv' with your desired output file name
            CSV.write("filtered_output.csv", filtered_df)
            println("Filtered CSV created successfully.")
        else
            println("No rows starting with 'D78A' in 'topic0x' were found in the CSV.")
        end
    catch e
        println("Error reading the CSV file")
    end
else
    println("The specified CSV file does not exist.")
end

data = CSV.read("C:/Users/ayesh/OneDrive/Documents/juliaproject3/filtered_output.csv", DataFrame)
newData = select(data, [:block_hash, :signed_at, :txhash, :topic0x, :topic1, :topic2, :data0, :data1, :data2, :data3, :datarest])
output_file = "filtered_output.csv"

CSV.write(output_file, newData)



#########

data = CSV.read("C:/Users/ayesh/OneDrive/Documents/juliaproject3/filtered_output.csv", DataFrame)
newData = select(data, [:txhash, :signed_at, :data0,:data1,:data2,:data3])
newData.data0_decimal = map(row -> (parse(BigInt, row.data0, base=16)) / 1E18, eachrow(newData))
newData.data1_decimal = map(row -> (parse(BigInt, row.data1, base=16)) / 1E18, eachrow(newData))
newData.data2_decimal = map(row -> (parse(BigInt, row.data2, base=16)) / 1E18, eachrow(newData))
newData.data3_decimal = map(row -> (parse(BigInt, row.data3, base=16)) / 1E18, eachrow(newData))

output_file = "new_data.csv"
CSV.write(output_file, newData)


#~~~~~~~~~~~~~~~~~~~~~~~
#### filters low and high

# Load the existing CSV file
existing_csv_file = "C:/Users/ayesh/OneDrive/Documents/juliaproject3/new_data.csv"

df = CSV.File(existing_csv_file) |> DataFrame

# Select the desired columns
df = select(df, [:txhash, :signed_at, :data1_decimal,:data2_decimal])


filtered_df = filter(row -> row.data1_decimal < 4000 && row.data2_decimal > 1E5, df)

# Sort the DataFrame by the "data0" column in ascending order
sorted_df = sort(filtered_df, :data2_decimal, rev=false)

# Keep only the top 10 rows
top_10_rows = first(sorted_df, 10)

# Save the filtered DataFrame to a new CSV file
new_csv_file = "topdata2.csv"
CSV.write(new_csv_file, top_10_rows)

println("Filtered CSV file created with the top 10 rows.")



#~~~~~~~~~~~~~~~


## filters only top
df = CSV.File(existing_csv_file) |> DataFrame

# Select the desired columns
df = select(df, [:txhash, :signed_at, :data1_decimal,:data2_decimal])


# Sort the DataFrame by the "data0" column in descending order
df = sort(df, :data2_decimal, rev=true)

# Keep only the top 10 rows
top_10_rows = first(df, 10)

# Save the filtered DataFrame to a new CSV file
new_csv_file = "topdata2.csv"
CSV.write(new_csv_file, top_10_rows)

println("Filtered CSV file created with the top 10 rows.")










#********************************************************************************************************
#finding tx_value vs time -> data0 line for DDF2 in topic0x

#top topic0s in terms of "DDF2"
file_path = "C:/Users/ayesh/OneDrive/Documents/juliaproject3/group3.csv"

# Check if the file exists
if isfile(file_path)
    try
        df = CSV.File(file_path) |> DataFrame
        filtered_df = filter(row -> startswith(row.topic0x, "DDF2"), df)
        
        if nrow(filtered_df) > 0
            # Replace 'filtered_output.csv' with your desired output file name
            CSV.write("filtered_output_tx_value.csv", filtered_df)
            println("Filtered CSV created successfully.")
        else
            println("No rows starting with 'D78A' in 'topic0x' were found in the CSV.")
        end
    catch e
        println("Error reading the CSV file")
    end
else
    println("The specified CSV file does not exist.")
end


data = CSV.read("C:/Users/ayesh/OneDrive/Documents/juliaproject3/filtered_output_tx_value.csv", DataFrame)
newData = select(data, [:block_hash, :txhash, :topic0x, :signed_at, :data0, :data1, :data2, :data3])
output_file = "filtered_output_tx_value.csv"

CSV.write(output_file, newData)
newData.data0_decimal = map(row -> (parse(BigInt, row.data0, base=16)) / 1E18, eachrow(newData))

#newData.data0_decimal = parse.(Float64, newData.data0_decimal)

newData = filter(row -> row.data0_decimal < 100.0, newData)
output_file = "filtered_output_tx_value.csv"

CSV.write(output_file, newData)


#print chart
# Load the CSV file into a DataFrame
df = CSV.File("filtered_output_tx_value.csv") |> DataFrame

df.signed_at = Dates.DateTime.(df.signed_at, "yyyy-mm-dd HH:MM:SS")
sortedTxValueDf = sort(df, :signed_at)



sortedTxValueDf.year = Dates.year.(sortedTxValueDf.signed_at)
sortedTxValueDf.month = Dates.month.(sortedTxValueDf.signed_at)
sortedTxValueDf.day = Dates.day.(sortedTxValueDf.signed_at)

averageTxValueDf = combine(groupby(sortedTxValueDf, [:year, :month, :day]),
    :data0_decimal => mean
)

averageTxValueDf.date = map(row -> Dates.DateTime(row.year, row.month, row.day), eachrow(averageTxValueDf))
select!(averageTxValueDf, Not([:year, :month, :day]))

avgTxValuePlot = plot(averageTxValueDf, x=:date, y=:data0_decimal_mean, Layout(title="Average Tx_Value Over Time", xaxis_title="Date", yaxis_title="Tx_Value", xaxis_rangeslider_visible=true))

#averageTxValueDf2 = combine(groupby(sortedTxValueDf, [:year, :month, :day]))
#averageTxValueDf2.date = map(row -> Dates.DateTime(row.year, row.month, row.day), eachrow(averageTxValueDf2))

#avgTxValuePlot = plot(averageTxValueDf2, x=:date, y=:data0_decimal, Layout(title="Average Tx_Value Over Time", xaxis_title="Date", yaxis_title="Tx_Value (ETH)", xaxis_rangeslider_visible=true))



#chart = @vlplot(
 #   data = df,
  #  mark = :line,
   # x = {"signed_at:T", axis = {title = "Time"}},
   # y = :data0_decimal,
    #width = 800,
    #height = 400,
    #title = "Tx_Value Used Over Time",
    
    #yaxis = {title = "Tx_value"}
#)

# Show the chart
display(chart)
save("C:/Users/ayesh/OneDrive/Desktop/plot.png", chart)


#***************************************************************************************************************

