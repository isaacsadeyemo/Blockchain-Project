module Utilities



using DataFrames
using CSV


# Removes all rows with duplicate entries in the specified column
function remove_duplicates(df::DataFrame, col::Symbol)
    return unique(df, col)
end


# Extracts one or more columns from a dataframe
function extract_columns(df::DataFrame, cols::Vector{Symbol})
    # Note that input col_symbols looks like this -> [:chain_id, :chain_name])
    return df[:, cols]
end


# Drops one or more columns from the input dataframe
function drop_columns(df::DataFrame, cols::Vector{Symbol})
    # Note that input col_symbols looks like this -> [:chain_id, :chain_name])
    for col in cols
        df = df[:, Not(col)]
    end
    # return df
end


# Returns a dataframe containing only the unique rows of a single column
function unique_column(df::DataFrame, col::Symbol)
    column = extract_columns(df, [col])
    unique_column = remove_duplicates(column, col)
    return unique_column
end


# Renames a specified column renamed
function rename(df::DataFrame, old_name::Symbol, new_name::Symbol)
    rename!(df, old_name => new_name)
end


# Renames a column at a specified index
function rename(df::DataFrame, index::Int, new_name::Symbol)
    rename!(df, index => new_name)
end


# Extracts the 10 rows with the highest values in a specified column
function top_n(df::DataFrame, col::Symbol, n::Int)
    sorted_df = sort(df, col, rev=true)
    return sorted_df[1:n, :]
end


# Extracts the 10 rows with the lowest values in a specified column
function bottom_n(df::DataFrame, col::Symbol, n::Int)
    sorted_df = sort(df, col)
    return sorted_df[1:n, :]
end


# Converts values to their decimal form and adds a new column of big floats in ETHER
function str_to_decimal(df::DataFrame, col::Symbol)
    f(val_str) = parse(BigInt, val_str) / (10^18)
    df.values = map(x -> f(x), df[:, col])
end


# Keeps only rows that contain the filter string in the specified column
function filter_contains(df::DataFrame, col::Symbol, filter_str::String)
    return filter(row -> contains(row[col], filter_str), df)
end


# Returns data for only those chains which are mainnets
function get_mainnet(df::DataFrame)
    return filter_contains(df, :chain_name, "mainnet")
end



end