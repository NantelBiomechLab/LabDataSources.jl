export V3DExport, V3DEvents, V3DExportSource, V3DEventsSource

export writeeventsfile

struct V3DExport; end
struct V3DEvents; end

const V3DExportSource = Source{V3DExport}
const V3DEventsSource = Source{V3DEvents}

function DatasetManager.readsource(s::V3DExportSource; kwargs...)
    return matread(sourcepath(s))
end

function DatasetManager.readsegment(seg::Segment{V3DExportSource};
    events::Union{Nothing,Vector}=nothing,
    series::Union{Nothing,Vector}=nothing
)
    file = matopen(sourcepath(seg.source))
    fs = only(read(file, "FRAME_RATE"))
    start, finish = seg.start, seg.finish

    # TODO: Check that seg.finish is <= the (time) length of the source

    outevents = Dict{String,Vector{Float64}}()
    if events !== nothing
        for event in events
            name = String(event)
            if exists(file, name)
                tmp = only(read(file, name))
                if tmp isa AbstractArray
                    outevents[name] = vec(tmp)
                else
                    outevents[name] = [tmp]
                end
                startidx = findfirst(>=(start), outevents[name])
                if startidx == 0
                    # @warn "no $event events during given start and end times"
                    delete!(outevents, name)
                    break
                end
                # Might throw, FIXME, see above
                finidx = isnothing(finish) ? lastindex(outevents[name]) : findlast(<=(finish), outevents[name])

                # Shift events to be index accurate for data subsection
                outevents[name] = outevents[name][startidx:finidx] .- start .+ (1/fs)
                sort!(outevents[name])
            else
                # @warn "Requested event $event does not exist in source data"
            end
        end
    end

    outseries = Dict{String,Matrix{Float64}}()
    if series !== nothing
        for t in series
            name = String(t)
            if exists(file, name)
                tmp = only(read(file, string(t)))
                len = size(tmp, 1)
                startidx = round(Int, start*fs)
                startidx += (startidx === 0) ? 1 : 0
                finidx = isnothing(finish) ? len : min(len, round(Int, finish*fs))
                outseries[name] = tmp[startidx:finidx, :]
            else
                @warn "Requested time series $t does not exist in source data"
            end
        end
    end

    close(file)

    return (fs, outevents, outseries)
end

function DatasetManager.readsource(s::V3DEventsSource;
    events::Union{Nothing,Vector{String}}=nothing, threaded=nothing, kwargs...
)
    events = CSV.File(sourcepath(s); header=2, datarow=6, select=events, drop=[1], threaded)

    return Dict(string(name) => sort!(collect(skipmissing(Tables.getcolumn(events, name))))
        for name in Tables.columnnames(events))
end

function deletefirst!(a::Vector, n::Integer)
    zero(n) < n < length(a) && Base._deletebeg!(a, n)
    return a
end

function DatasetManager.readsegment(seg::Segment{V3DEventsSource};
    events=nothing, threaded=nothing, kwargs...
)
    src = readsource(seg.source; events, threaded, kwargs...)

    if !isnothing(seg.start)
        foreach(src) do (k, v)
            f = searchsortedfirst(v, seg.start)
            if f ≤ length(v)
                deletefirst!(v, f-1)
            else
                empty!(v)
            end
            v .-= seg.start
        end
    end

    if !isnothing(seg.finish)
        foreach(src) do (k, v)
            l = searchsortedlast(v, seg.finish)
            resize!(v, l)
        end
    end

    return src
end

function generatesource(trial, src::V3DEventsSource, deps; genfunc)
    mkpath(dirname(sourcepath(src)))

    events = genfunc(trial)
    writeeventsfile(sourcepath(src), events)

    return src
end

function writeeventsfile(
    fn::String, events::Dict{K,<:AbstractVector}
) where K <: Union{Symbol, String}
    writeeventsfile(fn, zip(keys(events), values(events))...)
end

function writeeventsfile(
    fn::String, events::Vararg{Tuple{K,<:AbstractVector},N}
) where K <: Union{Symbol, String} where N
    (path, io) = mktemp()

    header = fill("", (5,1))
    header[5,1] = "ITEM"
    numevents = maximum(broadcast(x -> size(x[2],1), events))
    eventdata = string.(collect(1:numevents))

    file = splitext(basename(fn))[1]*".c3d"

    for event in events
        header = [ header [ file,
                            string(event[1]),
                            "EVENT_LABEL",
                            "ORIGINAL",
                            "X" ] ]
        tmp = fill("", (numevents))
        tmp[axes(event[2], 1)] .= string.(event[2])
        eventdata = [ eventdata tmp ]
    end

    CSV.write(fn, Tables.table([header; eventdata]); delim='\t')

    return nothing
end
