using DatasetManager, TextParse

export DFlowSource, RawDFlowPDSource

struct DFlowSource <: AbstractSource
    path::String
end

struct RawDFlowPDSource <: AbstractSource
    path::String
end

function DatasetManager.readsource(s::DFlowSource; threaded=true, kwargs...)
    # csvread(sourcepath(s); skiplines_begin=1, header_exists=true, kwargs...)
    CSV.File(sourcepath(s); header=2, threaded, kwargs...) |> DataFrames
end

function DatasetManager.readsource(s::RawDFlowPDSource; kwargs...)
    csvread(sourcepath(s), '\t'; skiplines_begin=7, header_exists=false, kwargs...)
    # CSV.File(sourcepath(s);
end

function DatasetManager.readsegment(
    seg::Segment{O}; kwargs...
) where O <: Union{DFlowSource,RawDFlowPDSource}
    timecol = (O isa DFlowSource) ? 1 : 2
    columns, colnames = readsource(seg.source; kwargs...)
    firsttime = first(columns[timecol])
    lasttime = last(columns[timecol])

    if isnothing(seg.finish)
        _finish = lasttime
    else
        _finish = seg.finish
    end

    firsttime ≤ seg.start ≤ lasttime || throw(error("$s start time $(seg.start) is not within"*
        "the source time range of $firsttime:$lasttime"))
    firsttime ≤ _finish ≤ lasttime || throw(error("$s finish time $(seg.finish) is not within "*
        "the source time range of $firsttime:$lasttime"))

    startidx = searchsortedfirst(columns[timecol], firsttime)

    if isnothing(seg.finish)
        finidx = lastindex(columns[timecol])
    else
        finidx = searchsortedlast(columns[timecol], seg.finish)
    end

    segcolumns = ntuple(i -> columns[i][startidx:finidx], length(columns))

    return segcolumns, colnames
end
