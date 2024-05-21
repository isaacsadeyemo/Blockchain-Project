module Queries

include("Utilities.jl")

using DataFrames, DataFramesMeta
using Statistics
using PDFIO
using Tables
using CSV
using Dates
using StatsBase

function blockheights(df::DataFrame)
    grouped_data = groupby(df, :block_height,)
    no_of_logs = combine(grouped_data,:tx_offset,:tx_gas_price,nrow)
    rename!(no_of_logs, :nrow => "Number_of_logs")
    logs = Utilities.top_n(no_of_logs,:Number_of_logs,20000)
    unique_df = unique!(logs)
    filtered_df = filter(row -> row.block_height == 11976043, unique_df)
    return filtered_df
end

function frequenttxhash(df::DataFrame)
    grouped_data = groupby(df, :txhash)
    no_of_logs = combine(grouped_data,nrow,:tx_sender,:tx_recipient)
    rename!(no_of_logs, :nrow => "Number_of_events")
    grouped_data2 = sort(no_of_logs,:Number_of_events,rev = true)
    unique_df = unique!(grouped_data2)
    return unique_df
   # return Utilities.top_n(no_of_logs,:Number_of_events,20)
end


function frequenttx(df::DataFrame)
    grouped_data = groupby(df, :txhash)
    no_of_logs = combine(grouped_data,nrow)
    rename!(no_of_logs, :nrow => "Number_of_events")
    grouped_data2 = sort(no_of_logs,:Number_of_events,rev = true)
    return grouped_data2
   # return Utilities.top_n(no_of_logs,:Number_of_events,20)
end

function sender(df::DataFrame)
    grouped_data = groupby(df, :tx_sender)
    no_of_logs = combine(grouped_data,nrow)
    rename!(no_of_logs, :nrow => "Occurences")
    return Utilities.top_n(no_of_logs,:Occurences,40)
end

function recipient(df::DataFrame)
    grouped_data = groupby(df, :tx_recipient)
    no_of_logs = combine(grouped_data,nrow)
    rename!(no_of_logs, :nrow => "Occurences")
    return Utilities.top_n(no_of_logs,:Occurences,20)
end

function gasoff(df::DataFrame)
    groupedfirst = groupby(df, :txhash)
    grouped_data = combine(groupedfirst, :tx_gas_offered, :signed_at)
    grouped_data[!, :signed_at] = DateTime.(grouped_data[!, :signed_at], dateformat"yyyy-mm-dd HH:MM:SS")
    grouped_data[!, :dates] = Date.(grouped_data[!, :signed_at])
    grouped_data1 = combine(groupby(grouped_data, :dates), :txhash => length, :tx_gas_offered => mean)
    grouped_data2 = Utilities.top_n(grouped_data1,:tx_gas_offered_mean, 30)
    grouped_data2 = sort(grouped_data2,:dates)
    return grouped_data2
end

function testerr(df::DataFrame)
    interactions = combine(groupby(df, [:tx_sender, :tx_recipient]), nrow => :interaction_count)
    return Utilities.top_n(interactions,:interaction_count,40)
end


end