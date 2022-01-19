export DFlow, RawDFlowPDSource

struct DFlow; end
struct RawDFlow; end
struct RawDFlowPD; end

const DFlowSource = Source{DFlow}
const RawDFlowPDSource = Source{RawDFlowPD}

function DatasetManager.readsource(s::DFlowSource; kwargs...)
    CSV.read(sourcepath(s), DataFrame; header=2, kwargs...)
end

function DatasetManager.readsource(s::Source{RawDFlow}; kwargs...)
    CSV.read(sourcepath(s), DataFrame; header=1, kwargs...)
end

function DatasetManager.readsource(s::Source{RawDFlowPD}; kwargs...)
    CSV.read(sourcepath(s), DataFrame; header=7, kwargs...)
end

function DatasetManager.readsegment(
    seg::Segment{O}; kwargs...
) where O <: Union{Source{DFlow},Source{RawDFlow}}
    timecol = (O isa DFlowSource) ? 1 : 2
    columns, colnames = readsource(seg.source; kwargs...)
    firsttime = first(columns[timecol])
    lasttime  = last(columns[timecol])

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
