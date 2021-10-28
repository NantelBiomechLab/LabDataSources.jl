export OSimModel, OSimMotion

struct OSimModel; end
srcname_default(::Type{Source{OSimModel}}) = "osim"

struct OSimMotion <: AbstractSource
    path::String
    compressed::Bool
end

function OSimMotion(path)
    if splitext(path)[2] == ".gz"
        compressed = true
    else
        compressed = false
    end

    OSimMotion(path, compressed)
end

DatasetManager.srcext(::Type{OSimMotion}) = ".mot"
DatasetManager.dependencies(::Type{OSimMotion}) = (Source{OSimModel}, Source{TRCFile})
DatasetManager.srcname_default(::Type{OSimMotion}) = "ik"

# \1 = frame, \2 = time, \3 = total squared error, \4 = RMS error, \5 = worst marker error,
# \6 = worst marker label
const IK_UPDATE_RGX = r"Frame (\d+)[^\d]+(\d+.\d+)\)[^\d]+(\d+.\d+)[^\d]+(\d+.\d+)[^\d]+(\d+.\d+) \(([^\)]+)"
const RST = Crayon(reset=true)

const terrstr = Printf.Format(
    "│ Total error²: %1.2f ± %1.2f \e[2mcm\e[0m   "*
    "Max error:  %s%1.2f ± %1.2f\e[0m \e[2mcm\e[0m (%1.2f … %1.2f) "*
    "\e[2m(min … max)\e[0m\n"
)
const rmserrstr = Printf.Format(
    "│ RMS error:    %s%1.2f ± %1.2f\e[0m \e[2mcm\e[0m   "*
    "Worst marker: %s @ %s%1.2f\e[0m \e[2mcm\e[0m (t=%.2f)\n"
)

function signalcolor(val,lo,hi)
    return val ≤ lo ? Crayon(foreground=:green)  :
           val ≤ hi ? Crayon(foreground=:yellow) :
                      Crayon(foreground=:red, bold=true)
end

function DatasetManager.generatesource(
    trial,
    src::OSimMotion,
    deps;
    iksetupxml=nothing,
    compress=false,
    logio=stdout,
    show_progress=true,
    starttime = -Inf,
    finishtime = Inf
)
    mkpath(dirname(sourcepath(src)))
    tmpdir = tempname()
    mkpath(tmpdir)

    model = getsource(trial, only(filter(x-> Source{OSimModel} === x ||
        (x isa Pair && Source{OSimModel} ∈ x), deps)))
    trc = getsource(trial, only(filter(x-> Source{TRCFile} === x ||
        (x isa Pair && Source{TRCFile} ∈ x), deps)))

    if isnothing(iksetupxml)
        redirect_stdout(devnull) do
            run(Cmd(`opensim-cmd print-xml InverseKinematicsTool`; dir=tmpdir))
        end
        iksetupxml = joinpath(tmpdir, "default_InverseKinematicsTool.xml")
        @assert isfile(iksetupxml)
    end
    iksetup = readxml(iksetupxml)
    nd = findfirst("//InverseKinematicsTool/model_file", iksetup)
    setnodecontent!(nd, sourcepath(model))

    nd = findfirst("//InverseKinematicsTool/output_motion_file", iksetup)
    setnodecontent!(nd, sourcepath(src))

    nd = findfirst("//InverseKinematicsTool/marker_file", iksetup)
    setnodecontent!(nd, sourcepath(trc))

    nd = findfirst("//InverseKinematicsTool/time_range", iksetup)
    setnodecontent!(nd, "$starttime $finishtime")

    iksetuppath = tempname(tmpdir)
    write(iksetuppath, iksetup)

    framerate, numframes = open(sourcepath(trc)) do io
        readline(io); readline(io)
        str = readline(io)
        m = match(r"(\d+)\.?\d*\s+(\d+\.?\d*\s+)(\d+)", str)
        parse(Int, m[1]), parse(Int, m[3])
    end
    if !isinf(starttime) || !isinf(finishtime)
        numframes = length(max(starttime, 0):inv(framerate):min(numframes/framerate, finishtime))
    end

    local_rgx = Regex(IK_UPDATE_RGX.pattern)
    println(IOContext(logio, :limit=>true), "╭ Running IK for $trial")
    open(Cmd(`opensim-cmd run-tool $iksetuppath`; dir=tmpdir)) do io
        prog = Progress(numframes;
            desc="│ Progress: ", enabled=show_progress, output=logio, dt = 1,
            showspeed=true)

        worstmarker = (;label="", err=0.0, time=0.0, frame=0)
        tsqerr = Series(Mean(), Variance())
        rmse = Series(Mean(), Variance())
        maxerr = Series(Mean(), Variance(), Extrema())
        worstmarkers = Dict{String,typeof(Mean())}()

        for line in eachline(io)
            m = match(local_rgx, line)
            if !isnothing(m)
                fit!(tsqerr, parse(Float64, m[3])*100)
                fit!(rmse, parse(Float64, m[4])*100)
                m5 = parse(Float64, m[5])*100
                m6 = string(m[6])
                fit!(get!(worstmarkers, m6, Mean()), m5)
                if m5 > value(maxerr)[3].max
                    worstmarker = (;label=m6, err=m5, time=parse(Float64, m[2]),
                                   frame=parse(Int, m[1]))
                end
                fit!(maxerr, m5)
                next!(prog)
            end
        end
        finish!(prog)

        maxcolor = signalcolor(value(maxerr)[1], 2, 4)
        Printf.format(logio, terrstr,
            value(tsqerr)[1], sqrt(value(tsqerr)[2]), maxcolor, value(maxerr)[1],
            sqrt(value(maxerr)[2]), value(maxerr)[3].min, value(maxerr)[3].max)

        rmscolor = signalcolor(value(rmse)[1], 1, 2)
        Printf.format(logio, rmserrstr,
            rmscolor, value(rmse)[1], sqrt(value(rmse)[2]), worstmarker.label,
            signalcolor(worstmarker.err, 2, 4), worstmarker.err, worstmarker.time)

        vals = collect(values(worstmarkers))
        nbad = length(vals)
        ks = collect(keys(worstmarkers))

        sp = sortperm(vals, by=(v -> v.n*value(v)), rev=true)
        print(logio, "│ Bad markers:")
        flag = true
        i = 0
        while i ≤ min(5,nbad-1)
            i += 1
            flag || print(logio, ", ")
            mod(i, 3) == 1 && print(logio, "\n│   ")
            v = value(vals[sp[i]])
            @printf(logio, "%s: %1.2f \e[2mcm (%i frames)\e[0m", ks[sp[i]],
                value(vals[sp[i]]), vals[sp[i]].n)
            flag = false
        end
        if i < length(vals)
            print(logio, " ($(nbad-i) markers not shown)")
        end
    end
    println(logio, "\n╰───────")
    isfile(sourcepath(src)) || throw(error("Inverse kinematics failed for $trial"))
    if compress
        open(sourcepath(src)*".gz", "w+") do outio
            open(sourcepath(src), "r") do inio
                write(outio, GzipCompressorStream(inio))
            end
        end
        rm(sourcepath(src))
        _src = OSimMotion(sourcepath(src)*".gz", compress)
    else
        _src = src
    end

    return _src
end

function DatasetManager.readsource(src::OSimMotion;
    series=nothing, drop=["time"], threaded=nothing
)
    if src.compressed
        data = CSV.File(transcode(GzipDecompressor, Mmap.mmap(sourcepath(src))); header=11,
            delim='\t', threaded, select=series, drop) |> DataFrame
    else
        data = CSV.File(sourcepath(src); header=11, delim='\t', threaded, select=series,
            drop) |> DataFrame
    end

    return data
end

function DatasetManager.readsegment(seg::Segment{OSimMotion}; series=nothing, threaded=nothing)
    if !isnothing(series)
        if "time" ∉ series
            push!(series, "time")
            cols = Not("time")
        else
            cols = Colon()
        end
    else
        cols = Not("time")
    end
    data = readsource(seg.source; series, threaded, drop=nothing)
    if !isnothing(seg.start)
        starti = searchsortedfirst(data[!, "time"], seg.start)
    else
        starti = firstindex(data, 1)
    end

    if !isnothing(seg.finish)
        lasti = searchsortedlast(data[!, "time"], seg.finish)
    else
        lasti = lastindex(data, 1)
    end

    return data[starti:lasti, cols]
end

