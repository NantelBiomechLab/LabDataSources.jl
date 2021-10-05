export TRCFile

DatasetManager.srcname_default(::Type{Source{C3DFile}}) = "c3d"
DatasetManager.srcext(::Type{Source{C3DFile}}) = ".c3d"

function DatasetManager.readsource(s::Source{C3DFile}; kwargs...)
    return readc3d(sourcepath(s); kwargs...)
end

struct TRCFile; end

function DatasetManager.readsource(s::Source{TRCFile}; threaded=nothing, select=nothing, drop=["Frame#"])
    colnames = open(sourcepath(s)) do io
        readline(io); readline(io); readline(io)
        ln = readline(io)
        split(ln, r"[,\s]")
    end

    trc = CSV.File(sourcepath(s); header=false, datarow=7, threaded) |> DataFrame
    while (length(colnames)-2) % 3 !== 0
        pop!(colnames)
    end
    mkrs = Dict("Frame#" => copy(trc[!, 1]),
        "Time" => copy(trc[!, 2]),
        ( string(colnames[i]) => Matrix(trc[!, i:i+2]) for i in 3:3:lastindex(colnames) )...)

    !isnothing(select) && delete!.(Ref(mkrs), setdiff(select, keys(mkrs)))
    !isnothing(drop) && delete!.(Ref(mkrs), drop)

    return mkrs
end

DatasetManager.dependencies(::Type{Source{TRCFile}}) = (Source{C3DFile},)
DatasetManager.srcname_default(::Type{Source{TRCFile}}) = "trc"
DatasetManager.srcext(::Type{Source{TRCFile}}) = ".trc"

function DatasetManager.generatesource(
    trial,
    src::Source{TRCFile},
    deps;
    f! = identity,
    kwargs...
)
    srcpath = sourcepath(src)
    mkpath(dirname(srcpath))

    c3dsrc = getsource(trial, only(filter(x-> Source{C3DFile} === x ||
        (x isa Pair && Source{C3DFile} ∈ x), deps)))
    c3dfl = readsource(c3dsrc)
    f!(c3dfl)
    trcpath = splitext(srcpath)[2] == ".trc" ? srcpath : srcpath*".trc"
    if c3dfl.groups[:POINT][String, :UNITS] == "mm"
        precision=2
    else
        precision=5
    end
    writetrc(trcpath, c3dfl; precision, kwargs...)

    return Source{TRCFile}(trcpath)
end

