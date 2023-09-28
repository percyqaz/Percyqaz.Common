namespace Percyqaz.Common

open System
open System.IO
open System.Diagnostics
open System.Text.RegularExpressions
open System.Collections.Generic
open System.Threading

[<AutoOpen>]
module Combinators =
    
    let K x _ = x

[<AutoOpen>]
module Operators =

    let inline (%%) (a: 'T) (b: 'T) = ((a % b) + b) % b

[<Measure>]
type ms
type Time = float32<ms>

type NYI = unit

type Setting<'T, 'Config> =
    {
        Set: 'T -> unit
        Get: unit -> 'T
        Config: 'Config
    }
    member this.Value with get() = this.Get() and set(v) = this.Set(v)
    override this.ToString() = sprintf "<%O, %A>" this.Value this.Config

type Setting<'T> = Setting<'T, unit>

module Setting =

    type Bounds<'T> = Bounds of min: 'T * max: 'T
    type Bounded<'T> = Setting<'T, Bounds<'T>>
    
    let simple (x: 'T) =
        let mutable x = x
        {
            Set = fun v -> x <- v
            Get = fun () -> x
            Config = ()
        }

    let make (set: 'T -> unit) (get: unit -> 'T) =
        {
            Set = set
            Get = get
            Config = ()
        }

    let map (after_get: 'T -> 'U) (before_set: 'U -> 'T) (setting: Setting<'T, 'Config>) =
        {
            Set = before_set >> setting.Set
            Get = after_get << setting.Get
            Config = setting.Config
        }

    let iter (f: 'T -> unit) (setting: Setting<'T, 'Config>) = f setting.Value
    let app (f: 'T -> 'T) (setting: Setting<'T, 'Config>) = setting.Value <- f setting.Value
    let trigger (action: 'T -> unit) (setting: Setting<'T, 'Config>) = { setting with Set = fun x -> setting.Set x; action x }

    let inline bound (min: 'T) (max: 'T) (setting: Setting<'T, 'Config>) =
        if min > max then invalidArg (nameof min) "min cannot be more than max"
        {
            Set =
                fun v ->
                    if v < min then setting.Set min
                    elif v > max then setting.Set max
                    else setting.Set v
            Get = setting.Get
            Config = Bounds (min, max)
        }

    let inline round (dp: int) (setting: Setting<float, 'Config>) =
        { setting with
            Set = fun v -> setting.Set (Math.Round (v, dp))
        }

    let inline roundf (dp: int) (setting: Setting<float32, 'Config>) =
        { setting with
            Set = fun v -> setting.Set (MathF.Round (v, dp))
        }
        
    let inline roundt (dp: int) (setting: Setting<float32<ms>, 'Config>) =
        { setting with
            Set = fun v -> setting.Set (MathF.Round (float32 v, dp) * 1.0f<ms>)
        }

    let alphaNum (setting: Setting<string, 'Config>) =
        let regex = Regex("[^\sa-zA-Z0-9_-]")
        map id (fun s -> regex.Replace(s, "")) setting

    let inline bounded x min max =
        simple x
        |> bound min max

    let percent x = bounded x 0.0 1.0 |> round 2
    let percentf x = bounded x 0.0f 1.0f |> roundf 2

    let f32 (setting: Setting<float, Bounds<float>>) = 
        let (Bounds (lo, hi)) = setting.Config
        {
            Set = float >> setting.Set
            Get = setting.Get >> float32
            Config = Bounds (float32 lo, float32 hi)
        }

type LoggingLevel = DEBUG = 0 | INFO = 1 | WARNING = 2 | ERROR = 3 | CRITICAL = 4
type LoggingEvent = LoggingLevel * string * string

type Logging() =
    static let evt = new Event<LoggingEvent>()
    static let mutable init_handle : IDisposable option = None

    static let mutable using_defaults = true
    static let mutable logFile = None
    static let mutable verbosity = LoggingLevel.DEBUG

    static let agent = new MailboxProcessor<LoggingEvent>(fun box -> async { while (true) do let! e = box.Receive() in evt.Trigger e })
    static do agent.Start()

    static member LogFile 
        with get() = logFile
        and set(value) = using_defaults <- false; logFile <- value

    static member Verbosity 
        with get() = verbosity
        and set(value) = using_defaults <- false; verbosity <- value

    static member Subscribe (f: LoggingEvent -> unit) = evt.Publish.Subscribe f
    static member Log level main details = 
        if init_handle.IsNone then init_handle <- Some <| Logging.Init()
        agent.Post (level, main, details.ToString())

    static member private Init() : IDisposable =
        if using_defaults then printfn "Initialising logging using defaults"

        let v = Logging.Verbosity
        let text_logger = 
            Logging.Subscribe 
                (fun (level, main, details) ->
                    if level >= v then 
                        printfn "[%A]: %s" level main
                        if level = LoggingLevel.CRITICAL then printfn " .. %s" details)

        match Logging.LogFile with
        | Some (f: string) ->

            Directory.CreateDirectory(Path.GetDirectoryName f) |> ignore
            let logfile = File.Open(f, FileMode.Append)
            let sw = new StreamWriter(logfile, AutoFlush = true)
            let file_writer = 
                Logging.Subscribe
                    ( fun (level, main, details) ->
                        if details = "" then sprintf "[%A] %s" level main else sprintf "[%A] %s\n%s" level main details
                        |> sw.WriteLine )
            
            { new IDisposable with override this.Dispose() = text_logger.Dispose(); sw.Close(); sw.Dispose(); logfile.Dispose(); file_writer.Dispose() }
        | None -> { new IDisposable with override this.Dispose() = text_logger.Dispose() }

    static member Info (s, err) = Logging.Log LoggingLevel.INFO s err
    static member Warn (s, err) = Logging.Log LoggingLevel.WARNING s err
    static member Error (s, err) = Logging.Log LoggingLevel.ERROR s err
    static member Debug (s, err) = Logging.Log LoggingLevel.DEBUG s err
    static member Critical (s, err) = Logging.Log LoggingLevel.CRITICAL s err

    static member Info s = Logging.Log LoggingLevel.INFO s ""
    static member Warn s = Logging.Log LoggingLevel.WARNING s ""
    static member Error s = Logging.Log LoggingLevel.ERROR s ""
    static member Debug s = Logging.Log LoggingLevel.DEBUG s ""
    static member Critical s = Logging.Log LoggingLevel.CRITICAL s ""

    static member Shutdown() =
        Thread.Sleep(200)
        while agent.CurrentQueueLength > 0 do
            Thread.Sleep(200)
        match init_handle with Some o -> o.Dispose() | None -> ()

[<AutoOpen>]
module Profiling =

    let private data = Dictionary<string, List<float>>()

    type ProfilingBuilder(name) =
        let sw = Stopwatch.StartNew()
        do if not (data.ContainsKey name) then data.[name] <- List<float>()
        member this.Delay(f) = f()
        member this.Combine(a, b) = b
        member this.Return x =
            data.[name].Add(sw.Elapsed.TotalMilliseconds)
            x
        member this.Zero() = this.Return()

    let profile name = new ProfilingBuilder(name)

    let dump_profiling_info() =
        for k in data.Keys do
            let items = data.[k]
            if items.Count > 0 then
                Logging.Debug(sprintf "%s: min %.4fms, max %.4fms, avg %.4fms, %i calls" k (Seq.min items) (Seq.max items) (Seq.average items) items.Count)
                items.Clear()

module Async =

    [<RequireQualifiedAccess>]
    type ServiceStatus =
        | Idle
        | Working
        | Busy
    
    /// Allows you to request some asynchronous work to be done, with a callback when it completes
    /// Results come back in the order they were requested
    [<AbstractClass>]
    type Service<'Request, 'Reply>() as this =

        let mutable working = false

        let worker = 
            MailboxProcessor<'Request * ('Reply -> unit)>.Start
                ( fun box -> 
                    let rec loop () = async {
                        working <- false
                        let! (request, callback) = box.Receive()
                        working <- true
                        try
                            let! res = this.Handle request
                            callback res
                        with err -> Logging.Critical(sprintf "Service exception -- %O" request, err)
                        return! loop ()
                    }
                    loop ()
                )
        
        abstract member Handle: 'Request -> Async<'Reply>
        
        member this.RequestAsync(req: 'Request) : Async<'Reply> =
            async {
                return! worker.PostAndAsyncReply(fun chan -> (req, fun reply -> chan.Reply reply))
            }

        member this.Request(req: 'Request, callback: 'Reply -> unit) : unit =
            worker.Post(req, callback)

        member this.Status = 
            if working then
                if worker.CurrentQueueLength > 0 then ServiceStatus.Busy else ServiceStatus.Working
            else ServiceStatus.Idle

    /// Allows you to request some asynchronous work to be done
    ///  If another job is requested before the first completes, the result of the outdated job is swallowed
    /// This allows easy reasoning about background jobs and how their results join with a single main thread
    [<AbstractClass>]
    type SwitchService<'Request, 'Reply>() as this =
        let mutable job_number = 0
        let lockObj = obj()
        let mutable result : (int * 'Reply) option = None

        let worker = 
            MailboxProcessor<int * 'Request>.Start
                ( fun box -> 
                    let rec loop () = async {
                        while box.CurrentQueueLength > 1 do let! _ = box.Receive() in ()
                        let! id, request = box.Receive()
                        try
                            let! processed = this.Process request
                            lock lockObj ( fun () -> result <- Some (id, processed) )
                        with err -> Logging.Error(sprintf "Error in request #%i: %O" id request, err)
                        return! loop ()
                    }
                    loop ()
                )

        /// This code runs asynchronously to fulfil the request
        abstract member Process: 'Request -> Async<'Reply>

        /// This code runs in whatever thread calls .Join()
        abstract member Handle: 'Reply -> unit
        
        /// Call this from the main thread to queue a new request. Any unhandled results from older requests are discarded
        member this.Request(req: 'Request) =
            lock lockObj ( fun () -> 
                job_number <- job_number + 1
                worker.Post(job_number, req)
            )

        /// Call this from the main thread to handle the result of the most recently completed request
        member this.Join() =
            lock lockObj ( fun () -> 
                match result with
                | Some (id, item) -> if id = job_number then this.Handle item
                | None -> ()
                result <- None
            )
                
    /// Allows you to request some asynchronous work to be done
    /// This version processes a sequence of items and runs a callback as each is completed
    ///  If another job is requested before the first completes, the remaining results of the outdated job are swallowed
    /// This allows easy reasoning about background jobs and how their results join with a single main thread
    [<AbstractClass>]
    type SwitchServiceSeq<'Request, 'Reply>() as this =
        let mutable job_number = 0
        let lockObj = obj()
        let mutable queue = []

        let worker = 
            MailboxProcessor<int * 'Request>.Start
                ( fun box -> 
                    let rec loop () = async {
                        let! id, request = box.Receive()
                        try
                            for processed in this.Process request do
                                lock lockObj ( fun () -> queue <- queue @ [id, processed] )
                            //lock lockObj ( fun () -> if id = job_number then this.JobCompleted(id, request) )
                        with err -> Logging.Error(sprintf "Error in #%i %O" id request, err)
                        return! loop ()
                    }
                    loop ()
                )
                
        /// This code runs asynchronously to fulfil the request
        abstract member Process: 'Request -> 'Reply seq
        
        /// This code runs in whatever thread calls .Join()
        abstract member Handle: 'Reply -> unit

        //abstract member JobCompleted: unit -> unit

        member this.Request(req: 'Request) =
            lock lockObj ( fun () ->
                job_number <- job_number + 1
                worker.Post(job_number, req)
            )

        /// Call this from the main thread to handle the result of the most recently completed request
        member this.Join() =
            lock lockObj ( fun () -> 
                for id, item in queue do
                    if id = job_number then this.Handle item
                queue <- []
            )