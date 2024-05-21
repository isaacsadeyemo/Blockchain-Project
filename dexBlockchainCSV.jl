using DataFrames
using CSV
using VegaLite
using Statistics
using Dates
using PlotlyJS

blockchainDf = CSV.read("/data/home/DefiClass2023/Dex/group3.csv", DataFrame)
#------------------------------------------------------------------------------------------

txGasDf = select(blockchainDf, [:block_hash, :signed_at, :tx_gas_spent, :tx_gas_offered])
dateFormat = Dates.DateFormat("yyyy-mm-dd HH:MM:SS")
txGasDf.signed_at = Dates.DateTime.(txGasDf.signed_at, dateFormat)
txGasDf.gasUtilRate = (txGasDf.tx_gas_spent ./ txGasDf.tx_gas_offered) .* 100

sortedTxGasDf = sort(txGasDf, :signed_at)

sortedTxGasDf.year = Dates.year.(sortedTxGasDf.signed_at)
sortedTxGasDf.month = Dates.month.(sortedTxGasDf.signed_at)
sortedTxGasDf.day = Dates.day.(sortedTxGasDf.signed_at)

averageTxGasDf = combine(groupby(sortedTxGasDf, [:year, :month, :day]),
    :gasUtilRate => mean,
)

averageTxGasDf.date = map(row -> Dates.DateTime(row.year, row.month, row.day), eachrow(averageTxGasDf))
select!(averageTxGasDf, Not([:year, :month, :day]))

gasUtilPlot = plot(averageTxGasDf, x=:date, y=:gasUtilRate_mean, Layout(title="Gas Utilization By Transaction", xaxis_title="Date", yaxis_title="Average Transaction Utilization (%)", xaxis_rangeslider_visible=true))
savefig(gasUtilPlot, "tx_gas_util_plot.html")

#------------------------------------------------------------------------------------------

txGasDf = select(blockchainDf, [:txhash, :signed_at, :tx_gas_spent])
dateFormat = Dates.DateFormat("yyyy-mm-dd HH:MM:SS")
txGasDf.signed_at = Dates.DateTime.(txGasDf.signed_at, dateFormat)
sortedTxGasDf = sort(txGasDf, :signed_at)

sortedTxGasDf.year = Dates.year.(sortedTxGasDf.signed_at)
sortedTxGasDf.month = Dates.month.(sortedTxGasDf.signed_at)
sortedTxGasDf.day = Dates.day.(sortedTxGasDf.signed_at)

averageTxGasDf = combine(groupby(sortedTxGasDf, [:year, :month, :day]),
    :tx_gas_spent => mean
)

averageTxGasDf.date = map(row -> Dates.DateTime(row.year, row.month, row.day), eachrow(averageTxGasDf))
select!(averageTxGasDf, Not([:year, :month, :day]))

gasSpentPlot = plot(averageTxGasDf, x=:date, y=:tx_gas_spent_mean, Layout(title="Gas Spent Over Time", xaxis_title="Date", yaxis_title="Gas Spent", xaxis_rangeslider_visible=true))
savefig(gasSpentPlot, "tx_gas_spent_plot.html")

#------------------------------------------------------------------------------------------

sandwichDf = select(blockchainDf, [:block_hash, :signed_at, :tx_offset, :txhash, :tx_sender, :tx_recipient])
dateFormat = Dates.DateFormat("yyyy-mm-dd HH:MM:SS")
sandwichDf.signed_at = Dates.DateTime.(sandwichDf.signed_at, dateFormat)
sortedSandwichDf = sort(sandwichDf, :signed_at)

possibleSandwichDf = DataFrame()

i = 2
sandwichAttackCount = 0 # 851 Total
while i <= size(sortedSandwichDf, 1) - 1
    if (sortedSandwichDf.tx_sender[i] != sortedSandwichDf.tx_sender[i - 1]) && (sortedSandwichDf.tx_sender[i] != sortedSandwichDf.tx_sender[i + 1]) && (sortedSandwichDf.tx_sender[i - 1] == sortedSandwichDf.tx_sender[i + 1]) && (sortedSandwichDf.tx_offset[i] > sortedSandwichDf.tx_offset[i - 1])
        push!(possibleSandwichDf, sortedSandwichDf[i - 1, :])
        push!(possibleSandwichDf, sortedSandwichDf[i, :])
        push!(possibleSandwichDf, sortedSandwichDf[i + 1, :])
        sandwichAttackCount += 1
        i += 2
    else
        i += 1
    end
end

CSV.write("sandwich.csv", possibleSandwichDf)

senderCountDf = combine(groupby(possibleSandwichDf, [:tx_sender]),
    nrow => :txSenderCount
)
sortedSenderCountDf = first(sort(senderCountDf, :txSenderCount, rev = true), 10)

topSandwichAttackers = plot(sortedSenderCountDf, kind="bar", x=:tx_sender, y=:txSenderCount, color=:tx_sender, Layout(title="Top 10 Possible Sandwich Attackers", xaxis_title="Address", yaxis_title="Number Of Attacks", xaxis_categoryorder="total descending"))
savefig(topSandwichAttackers, "sandwich_attack_plot.html")


possibleSandwichDf.year = Dates.year.(possibleSandwichDf.signed_at)
possibleSandwichDf.month = Dates.month.(possibleSandwichDf.signed_at)
possibleSandwichDf.day = Dates.day.(possibleSandwichDf.signed_at)

sandwichCountDf = combine(groupby(possibleSandwichDf, [:year, :month, :day]), nrow)
sandwichCountDf.date = map(row -> Dates.DateTime(row.year, row.month, row.day), eachrow(sandwichCountDf))
select!(sandwichCountDf, Not([:year, :month, :day]))

sandwichCountPlot = plot(sandwichCountDf, x=:date, y=:nrow, Layout(title="Possible Sandwich Attacks Over Time", xaxis_title="Date", yaxis_title="Sandwich Attack Count", xaxis_rangeslider_visible=true))
savefig(sandwichCountPlot, "sandwich_count_plot.html")

#------------------------------------------------------------------------------------------

txCountDf = select(blockchainDf, [:block_hash, :signed_at, :txhash, :tx_sender, :tx_recipient, :tx_gas_spent, :tx_gas_offered])
dateFormat = Dates.DateFormat("yyyy-mm-dd HH:MM:SS")
txCountDf.signed_at = Dates.DateTime.(txCountDf.signed_at, dateFormat)

sortedTxCountDf = sort(txCountDf, :signed_at)

sortedTxCountDf.year = Dates.year.(sortedTxCountDf.signed_at)
sortedTxCountDf.month = Dates.month.(sortedTxCountDf.signed_at)
sortedTxCountDf.day = Dates.day.(sortedTxCountDf.signed_at)

txEventCountDf = combine(groupby(sortedTxCountDf, [:year, :month, :day]), nrow)
txEventCountDf.date = map(row -> Dates.DateTime(row.year, row.month, row.day), eachrow(txEventCountDf))
select!(txEventCountDf, Not([:year, :month, :day]))

txEventCountPlot = plot(txEventCountDf, x=:date, y=:nrow, Layout(title="Transaction Event Activity Over Time", xaxis_title="Date", yaxis_title="Transaction Activity Count", xaxis_rangeslider_visible=true))
savefig(txEventCountPlot, "tx_event_count_plot.html")

#------------------------------------------------------------------------------------------