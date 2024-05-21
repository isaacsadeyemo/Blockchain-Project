module Plotter

include("Queries.jl")
include("Utilities.jl")
using DataFrames, DataFramesMeta
using Statistics
using VegaLite
using Printf


function toprec(df::DataFrame)
data = Queries.recipient(df)
plot = data |> @vlplot(
        mark = "bar",
        encoding = {
            y ={field ="tx_recipient", type = "nominal"},
            x ={field="Occurences", type="quantitative", title="Recipient Occurences"},
            },
        title="Most frequent Recipients",
        width=400,
        height=300 
        #color={field="tx_failure_ratio", type="quantitative"},
    ) 
    
    save_plot(plot, "toprec.pdf", true)
end

function gasoffs(df::DataFrame)
    data = Queries.gasoff(df)
    plot = data |> @vlplot(
            mark = "line",
            encoding = {
                y ={field ="tx_gas_offered_mean", type = "quantitative", title="Average Tx gas offered"},
                x ={field="dates", type="nominal", title="Transaction Dates"},
                },
            title="Average tx gas offered by date",
            width=400,
            height=300 
            #color={field="tx_failure_ratio", type="quantitative"},
        ) 
        save_plot(plot, "Txdate.pdf", true)
    end

function blockheight(df::DataFrame)
    data = Queries.blockheights(df)
    plot = data |> @vlplot(
            mark = "line",
            encoding = {
                y ={field = "tx_gas_price", type = "quantitative"},
                x ={field= "tx_offset", type="nominal", title="Transaction Offset"},
                },
            title= "Block 11976043 gas price by transaction offset",
            width=400,
            height=300 
            #color={field="tx_failure_ratio", type="quantitative"},
    ) 
        save_plot(plot, "11976043.pdf", true)
    end

function topsend(df::DataFrame)
    data = Queries.sender(df)
    plot = data |> @vlplot(
            mark = "bar",
            encoding = {
                y ={field ="tx_sender", type = "nominal"},
                x ={field="Occurences", type="quantitative", title="Sender Occurences"},
                },
            title="Most frequent senders",
            width=600,
            height=300 
            #color={field="tx_failure_ratio", type="quantitative"},
        ) 
        
        save_plot(plot, "topsend.pdf", true)
    end
function topsend(df::DataFrame)
    data = Queries.sender(df)
    plot = data |> @vlplot(
            mark = "bar",
            encoding = {
                y ={field ="tx_sender", type = "nominal"},
                x ={field="Occurences", type="quantitative", title="Sender Occurences"},
                },
            title="Most frequent senders",
            width=700,
            height=300 
            #color={field="tx_failure_ratio", type="quantitative"},
        ) 
        
        save_plot(plot, "topsend.pdf", true)
    end

function save_plot(plot, name::String, overwrite::Bool)
    path = "plots2/" * name

    if overwrite
        if isfile(path)
            @printf "Overwriting plot %s...\n" name
        else
            @printf "Saving plot %s...\n" name
        end
        save(path, plot)
    else
        if isfile(path)
            @printf "Could not save plot %s, file already exists...\n" name
        else
            @printf "Saving plot %s...\n" name
            save(path, plot)
        end
    end
end


end