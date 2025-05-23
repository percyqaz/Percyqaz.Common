﻿namespace Percyqaz.Common

open System
open System.IO
open System.Diagnostics
open System.Text.RegularExpressions
open System.Collections.Generic
open System.Threading
open System.Runtime.CompilerServices

module Timestamp =

    let now () : int64 =
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

    let from_datetime (dt: DateTime) : int64 =
        (DateTimeOffset.op_Implicit <| dt.ToUniversalTime()).ToUnixTimeMilliseconds()

    let to_datetimeoffset (ts: int64) : DateTimeOffset =
        DateTimeOffset.FromUnixTimeMilliseconds(ts)

    let to_datetime (ts: int64) : DateTime = to_datetimeoffset(ts).UtcDateTime

    let since (ts: int64) : TimeSpan = TimeSpan.FromMilliseconds(now () - ts |> float)

[<AutoOpen>]
module Combinators =

    let K (x: 'T) (_: _) : 'T = x

    let fork (f: 'T -> unit) (g: 'T -> unit) : 'T -> unit =
        fun t ->
            f t
            g t

    let debug (x: 'A) : 'A =
        printfn "%A" x
        x

    // modulo instead of remainder
    let inline (%%) (a: 'T) (b: 'T) : 'T = ((a % b) + b) % b

    let expect (res: Result<'A, 'B>) : 'A =
        match res with
        | Ok ok -> ok
        | Error e -> failwithf "Expected success, got: %O" e

    let inline floor_uom (f: float32<'u>) : float32<'u> = f |> float32 |> floor |> LanguagePrimitives.Float32WithMeasure

    let lerp (x: float32) (a: float32) (b: float32) : float32 = (b - a) * x + a

[<Measure>]
type ms

type Time = float32<ms>

[<Measure>]
type rate

type NYI = unit

type Setting<'T, 'Config> =
    {
        Set: 'T -> unit
        Get: unit -> 'T
        Config: 'Config
    }
    member this.Value
        with get () = this.Get()
        and set (v) = this.Set(v)

    override this.ToString() : string =
        sprintf "<%O, %A>" this.Value this.Config

type Setting<'T> = Setting<'T, unit>

module Setting =

    type Bounds<'T> = 'T * 'T
    type Bounded<'T> = Setting<'T, Bounds<'T>>

    let simple (x: 'T) =
        let mutable x = x

        {
            Set = fun v -> x <- v
            Get = fun () -> x
            Config = ()
        }

    let make (set: 'T -> unit) (get: unit -> 'T) : Setting<'T> = { Set = set; Get = get; Config = () }

    let map (after_get: 'T -> 'U) (before_set: 'U -> 'T) (setting: Setting<'T, 'Config>) : Setting<'U, 'Config> =
        {
            Set = before_set >> setting.Set
            Get = after_get << setting.Get
            Config = setting.Config
        }

    let iter (f: 'T -> unit) (setting: Setting<'T, 'Config>) : unit = f setting.Value
    let app (f: 'T -> 'T) (setting: Setting<'T, 'Config>) : unit = setting.Value <- f setting.Value

    let trigger (action: 'T -> unit) (setting: Setting<'T, 'Config>) : Setting<'T, 'Config> =
        { setting with
            Set =
                fun x ->
                    setting.Set x
                    action setting.Value
        }

    let inline bound (min: 'T, max: 'T) (setting: Setting<'T, 'Config>) : Setting<'T, Bounds<'T>> =
        if min > max then
            invalidArg (nameof min) (sprintf "min (%O) cannot be more than max (%O)" min max)

        {
            Set =
                fun v ->
                    if v < min then setting.Set min
                    elif v > max then setting.Set max
                    else setting.Set v
            Get = setting.Get
            Config = min, max
        }

    let inline round (dp: int) (setting: Setting<float, 'Config>) : Setting<float, 'Config> =
        { setting with
            Set = fun v -> setting.Set(Math.Round(v, dp))
        }

    let inline roundf (dp: int) (setting: Setting<float32, 'Config>) : Setting<float32, 'Config> =
        { setting with
            Set = fun v -> setting.Set(MathF.Round(v, dp))
        }

    let inline roundf_uom (dp: int) (setting: Setting<float32<'u>, 'Config>) : Setting<float32<'u>, 'Config> =
        { setting with
            Set = fun v -> setting.Set(MathF.Round(float32 v, dp) |> LanguagePrimitives.Float32WithMeasure)
        }

    let alphanumeric (setting: Setting<string, 'Config>) : Setting<string, 'Config> =
        let regex = Regex(@"[^\sa-zA-Z0-9'_-]")
        map id (fun s -> regex.Replace(s, "")) setting

    let inline bounded (min: 'T, max: 'T) (x: 'T) : Bounded<'T> = simple x |> bound (min, max)

    let percent (x: float) : Bounded<float> =
        x |> bounded (0.0, 1.0) |> round 2

    let percentf (x: float32) : Bounded<float32> =
        x |> bounded (0.0f, 1.0f) |> roundf 2

    let f32 (setting: Bounded<float>) : Bounded<float32> =
        let lo, hi = setting.Config

        {
            Set = float >> setting.Set
            Get = setting.Get >> float32
            Config = Bounds(float32 lo, float32 hi)
        }

    let uom (setting: Bounded<float32<'u>>) : Bounded<float32> =
        let lo, hi = setting.Config

        {
            Set = fun v -> setting.Set (LanguagePrimitives.Float32WithMeasure v)
            Get = fun () -> float32 setting.Value
            Config = Bounds(float32 lo, float32 hi)
        }

type LoggingLevel =
    | DEBUG = 0
    | INFO = 1
    | WARNING = 2
    | ERROR = 3
    | CRITICAL = 4

type LoggingEvent = LoggingLevel * string

type Logging() =
    static let evt = new Event<LoggingEvent>()
    static let mutable init_handle: IDisposable option = None

    static let mutable using_defaults = true
    static let mutable log_file = None
    static let mutable verbosity = LoggingLevel.DEBUG

    static let agent =
        new MailboxProcessor<LoggingEvent>(fun box ->
            async {
                while (true) do
                    let! e = box.Receive()
                    evt.Trigger e
            }
        )

    static do agent.Start()

    static member LogFile
        with get () = log_file
        and set (value) =
            using_defaults <- false
            log_file <- value

    static member Verbosity
        with get () = verbosity
        and set (value) =
            using_defaults <- false
            verbosity <- value

    static member Subscribe(f: LoggingEvent -> unit) = evt.Publish.Subscribe f

    static member Log level message =
        if init_handle.IsNone then
            lock agent (fun () ->
                if init_handle.IsNone then
                    init_handle <- Some <| Logging.Init()
            )

        agent.Post(level, message)

    static member private Init() : IDisposable =
        if using_defaults then
            printfn "Initialising logging using defaults"

        let v = Logging.Verbosity

        let text_logger =
            Logging.Subscribe(fun (level, message) ->
                if level >= v then
                    match level with
                    | LoggingLevel.DEBUG -> printf "\x1b[2m"
                    | LoggingLevel.WARNING -> printf "\x1b[93m"
                    | LoggingLevel.ERROR -> printf "\x1b[91m"
                    | LoggingLevel.CRITICAL -> printf "\x1b[94m"
                    | _ -> ()
                    printfn "[%s] [%A] %s\x1b[0m" (DateTime.Now.ToString("HH:mm")) level message
            )

        match Logging.LogFile with
        | Some(f: string) ->

            Directory.CreateDirectory(Path.GetDirectoryName f) |> ignore
            let logfile = File.Open(f, FileMode.Append)
            let sw = new StreamWriter(logfile, AutoFlush = true)

            let file_writer =
                Logging.Subscribe(fun (level, message) ->
                    sprintf "[%s] [%A] %s" (DateTime.Now.ToString("HH:mm")) level message
                    |> sw.WriteLine
                )

            { new IDisposable with
                override this.Dispose() =
                    text_logger.Dispose()
                    sw.Close()
                    sw.Dispose()
                    logfile.Dispose()
                    file_writer.Dispose()
            }
        | None ->
            { new IDisposable with
                override this.Dispose() = text_logger.Dispose()
            }

    static member Info fmt = Printf.ksprintf (fun s -> Logging.Log LoggingLevel.INFO s) fmt
    static member Warn fmt = Printf.ksprintf (fun s -> Logging.Log LoggingLevel.WARNING s) fmt
    static member Error fmt = Printf.ksprintf (fun s -> Logging.Log LoggingLevel.ERROR s) fmt
    static member Debug fmt = Printf.ksprintf (fun s -> Logging.Log LoggingLevel.DEBUG s) fmt
    static member Critical fmt = Printf.ksprintf (fun s -> Logging.Log LoggingLevel.CRITICAL s) fmt

    static member Shutdown() =
        Thread.Sleep(200)

        while agent.CurrentQueueLength > 0 do
            Thread.Sleep(200)

        match init_handle with
        | Some o -> o.Dispose()
        | None -> ()

[<AutoOpen>]
module Profiling =

    let private data = Dictionary<string, List<float>>()

    let profile name func =
        let sw = Stopwatch.StartNew()

        if not (data.ContainsKey name) then
            data.[name] <- ResizeArray()

        func ()
        data.[name].Add(sw.Elapsed.TotalMilliseconds)

    let dump_profiling_info () =
        for k in data.Keys do
            let items = data.[k]

            if items.Count > 0 then
                Logging.Debug
                    "%s: min %.4fms, max %.4fms, avg %.4fms, %i calls"
                    k
                    (Seq.min items)
                    (Seq.max items)
                    (Seq.average items)
                    items.Count

                items.Clear()

module Async =

    [<RequireQualifiedAccess>]
    type QueueStatus =
        | Idle
        | Working
        | Busy

    /// Allows you to request some asynchronous work to be done, with a callback when it completes
    /// Results come back in the order they were requested
    [<AbstractClass>]
    type Queue<'Request, 'Reply>() as this =

        let mutable working = false

        let worker =
            MailboxProcessor<'Request * ('Reply -> unit)>.Start(fun box ->
                async {
                    while true do
                        working <- false
                        let! (request, callback) = box.Receive()
                        working <- true

                        try
                            let! res = this.Handle request
                            callback res
                        with err ->
                            Logging.Critical "Service exception in %O: %O" this err
                }
            )

        abstract member Handle: 'Request -> Async<'Reply>

        member this.RequestAsync(req: 'Request) : Async<'Reply> =
            async { return! worker.PostAndAsyncReply(fun channel -> (req, (fun reply -> channel.Reply reply))) }

        member this.Request(req: 'Request, callback: 'Reply -> unit) : unit = worker.Post(req, callback)

        member this.Status : QueueStatus =
            if working then
                if worker.CurrentQueueLength > 0 then
                    QueueStatus.Busy
                else
                    QueueStatus.Working
            else
                QueueStatus.Idle

    /// Allows you to request some asynchronous work to be done
    ///  If another job is requested before the current completes, the result of the outdated job is swallowed
    ///  If many jobs are requested before the current job completes, all but the latest are swallowed
    /// This allows easy reasoning about background jobs and how their results join with a single main thread
    [<AbstractClass>]
    type CancelQueue<'Request, 'Reply>() as this =
        let mutable job_number = 0
        let LOCK_OBJ = obj ()
        let mutable result: (int * 'Reply) option = None

        let worker =
            MailboxProcessor<int * 'Request>.Start(fun box ->
                async {
                    while true do
                        while box.CurrentQueueLength > 1 do
                            let! _ = box.Receive()
                            ()

                        let! id, request = box.Receive()

                        try
                            let! processed = this.Process request
                            lock LOCK_OBJ (fun () -> result <- Some(id, processed))
                        with err ->
                            Logging.Error "Error in request #%i of %O: %O" id this err
                }
            )

        /// This code runs asynchronously to fulfil the request
        abstract member Process: 'Request -> Async<'Reply>

        /// This code runs in whatever thread calls .Join()
        abstract member Handle: 'Reply -> unit

        /// Call this from the main thread to queue a new request. Any unhandled results from older requests are discarded
        member this.Request(req: 'Request) : unit =
            lock
                LOCK_OBJ
                (fun () ->
                    job_number <- job_number + 1
                    worker.Post(job_number, req)
                )

        /// Call this from the main thread to handle the result of the most recently completed request
        member this.Join() : unit =
            lock
                LOCK_OBJ
                (fun () ->
                    match result with
                    | Some(id, item) ->
                        if id = job_number then
                            this.Handle item
                    | None -> ()

                    result <- None
                )

    /// Allows you to request some asynchronous work to be done
    /// This version processes a sequence of items and runs a callback as each is completed
    ///  If another job is requested before the current job completes, the remaining results of the outdated job are swallowed
    ///  If many jobs are requested before the current job completes, all but the latest are swallowed
    /// This allows easy reasoning about background jobs and how their results join with a single main thread
    [<AbstractClass>]
    // todo: replace 'reply with unit -> unit
    type CancelQueueSeq<'Request, 'Reply>() as this =
        let mutable job_number = 0
        let LOCK_OBJ = obj ()
        let mutable queue = []

        let worker =
            MailboxProcessor<int * 'Request>.Start(fun box ->
                async {
                    while true do
                        while box.CurrentQueueLength > 1 do
                            let! _ = box.Receive()
                            ()

                        let! id, request = box.Receive()

                        try
                            for processed in this.Process request do
                                lock LOCK_OBJ (fun () -> queue <- queue @ [ id, processed ])
                        with err ->
                            Logging.Error "Error in request #%i of %O: %O" id this err
                }
            )

        /// This code runs asynchronously to fulfil the request
        abstract member Process: 'Request -> 'Reply seq

        /// This code runs in whatever thread calls .Join()
        abstract member Handle: 'Reply -> unit

        member this.Request(req: 'Request) : unit =
            lock
                LOCK_OBJ
                (fun () ->
                    job_number <- job_number + 1
                    worker.Post(job_number, req)
                )

        member this.Cancel() : unit =
            lock
                LOCK_OBJ
                (fun () ->
                    job_number <- job_number + 1
                )

        /// Call this from the main thread to handle the result of the most recently completed request
        member this.Join() : unit =
            lock
                LOCK_OBJ
                (fun () ->
                    for id, item in queue do
                        if id = job_number then
                            this.Handle item

                    queue <- []
                )