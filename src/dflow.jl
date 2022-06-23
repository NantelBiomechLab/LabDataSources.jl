export DFlow, RawDFlowPD

struct DFlow; end
struct RawDFlow; end
struct RawDFlowPD; end

function DatasetManager.readsource(s::Source{DFlow}; kwargs...)
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
) where O <: Union{Source{DFlow},Source{RawDFlow},Source{RawDFlowPD}}
    timecol = (O isa Source{DFlow}) ? 1 : 2
    df = readsource(seg.source; kwargs...)
    firsttime = first(df[!,timecol])
    lasttime  = last(df[!,timecol])

    firsttime ≤ something(seg.start, firsttime) ≤ lasttime ||
        throw(error("$seg start time $(seg.start) is not within the source time range of $firsttime:$lasttime"))
    firsttime ≤ something(seg.finish, lasttime) ≤ lasttime ||
        throw(error("$seg finish time $(seg.finish) is not within the source time range of $firsttime:$lasttime"))

    startidx = searchsortedfirst(df[!, timecol], something(seg.start, firsttime))

    if isnothing(seg.finish)
        finidx = lastindex(df[!, timecol])
    else
        finidx = searchsortedlast(df[!, timecol], seg.finish)
    end

    return df[startidx:finidx,:]
end
