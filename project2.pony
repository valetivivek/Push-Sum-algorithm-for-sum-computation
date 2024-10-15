use "collections"
use "random"
use "time"

use @exit[None](status: I32)

actor Main
  let _env: Env
  var _informed_count: USize
  var _stopped_count: USize
  var _total_nodes: USize
  let _timers: Timers = Timers
  var _is_complete: Bool = false
  let _finalizer: Finalizer
  var _start_time: U64

  new create(env: Env) =>
    _env = env
    _informed_count = 0
    _stopped_count = 0
    _total_nodes = 0
    _finalizer = Finalizer(this)
    _start_time = 0

    try
      let args = env.args
      if args.size() != 4 then
        error
      end

      let algorithm = args(1)?
      let num = args(2)?.u32()?
      let topology = args(3)?

      _total_nodes = num.usize()
      _start_time = Time.nanos()

      match algorithm
      | "Gossip" =>
        let gossip = Gossip(this)
        gossip.run(_total_nodes, topology)
      | "PushSum" =>
        let pushsum = PushSum(this)
        pushsum.run(_total_nodes, topology)
      else
        error
      end
    else
      usage()
    end

  fun usage() =>
    _env.out.print("Usage: program <algorithm> <num_nodes> <topology>")
    _env.exitcode(1)

  be node_updated(id: String, is_informed: Bool = false) =>
    if not _is_complete then
      if is_informed then
        _informed_count = _informed_count + 1
        _env.out.print("Node " + id + " informed. Total: " + _informed_count.string() + "/" + _total_nodes.string())
        if _informed_count == _total_nodes then
          _env.out.print("All nodes informed!")
        end
      else
        _stopped_count = _stopped_count + 1
        _env.out.print("Node " + id + " stopped/terminated. Total: " + _stopped_count.string() + "/" + _total_nodes.string())
        if _stopped_count == _total_nodes then
          algorithm_complete()
        end
      end
    end

  be node_informed(id: String) =>
    node_updated(id, true)

  be node_stopped(id: String) =>
    node_updated(id, false)

  be node_terminated(id: String) =>
    node_updated(id, false)

  be check_progress() =>
    if not _is_complete then
      _env.out.print("Progress: " + _informed_count.string() + "/" + _total_nodes.string() + " nodes informed, " +
        _stopped_count.string() + "/" + _total_nodes.string() + " nodes stopped/terminated")
    end

  be algorithm_complete() =>
    if not _is_complete then
      _is_complete = true
      let end_time = Time.nanos()
      let convergence_time = (end_time - _start_time).f64() / 1e6
      _env.out.print("Algorithm complete. Total nodes stopped/terminated: " + _stopped_count.string() + "/" + _total_nodes.string())
      _env.out.print("Convergence time: " + convergence_time.string() + " milliseconds")
      _timers.dispose()
      _env.out.print("Exiting program")
      _finalizer.finalize(0)
    end

  be timeout() =>
    if not _is_complete then
      _is_complete = true
      let end_time = Time.nanos()
      let elapsed_time = (end_time - _start_time).f64() / 1e6
      _env.out.print("Timeout reached. Nodes informed/terminated: " + _stopped_count.string() + "/" + _total_nodes.string())
      _env.out.print("Elapsed time: " + elapsed_time.string() + " milliseconds")
      _timers.dispose()
      _env.out.print("Exiting program")
      _finalizer.finalize(1)
    end

  be out(msg: String) =>
    _env.out.print(msg)

actor Finalizer
  let _main: Main tag
  let _timers: Timers = Timers

  new create(main: Main tag) =>
    _main = main

  be finalize(exit_code: I32) =>
    _timers(Timer(ExitNotify(exit_code), 1_000_000_000))

class ExitNotify is TimerNotify
  let _exit_code: I32

  new iso create(exit_code: I32) =>
    _exit_code = exit_code

  fun ref apply(timer: Timer, count: U64): Bool =>
    @exit(_exit_code)
    false

interface NodeLike
  be add_neighbor(neighbor: NodeLike tag)
  be start()
  be receive_message(msg: (String | (F64, F64)))

primitive TopologyFunctions[N: NodeLike tag]
  fun fullnetwork(nodes: Array[N]) =>
    for i in nodes.values() do
      for j in nodes.values() do
        if i isnt j then
          i.add_neighbor(j)
        end
      end
    end

  fun line(nodes: Array[N])? =>
    let size = nodes.size()
    for i in Range(0, size) do
      if i > 0 then nodes(i)?.add_neighbor(nodes(i-1)?) end
      if i < (size-1) then nodes(i)?.add_neighbor(nodes(i+1)?) end
    end

  fun grid3d(nodes: Array[N])? =>
    let size = nodes.size()
    let side = (size.f64().cbrt().ceil()).usize()
    for i in Range(0, size) do
      let x = i % side
      let y = (i / side) % side
      let z = i / (side * side)
      if x > 0 then nodes(i)?.add_neighbor(nodes(i-1)?) end
      if (x < (side-1)) and ((i+1) < size) then nodes(i)?.add_neighbor(nodes(i+1)?) end
      if y > 0 then nodes(i)?.add_neighbor(nodes(i-side)?) end
      if (y < (side-1)) and ((i+side) < size) then nodes(i)?.add_neighbor(nodes(i+side)?) end
      if z > 0 then nodes(i)?.add_neighbor(nodes(i-(side*side))?) end
      if (z < (side-1)) and ((i+(side*side)) < size) then nodes(i)?.add_neighbor(nodes(i+(side*side))?) end
    end

  fun imperfect3d(nodes: Array[N])? =>
    grid3d(nodes)?
    let size = nodes.size()
    let rand = Rand(Time.nanos())
    for i in Range(0, size) do
      var random_neighbor = i
      while random_neighbor == i do
        random_neighbor = rand.int(size.u64()).usize()
      end
      nodes(i)?.add_neighbor(nodes(random_neighbor)?)
    end

  fun create_topology(nodes: Array[N], topology: String)? =>
    match topology
    | "fullnetwork" => fullnetwork(nodes)
    | "3d" => grid3d(nodes)?
    | "line" => line(nodes)?
    | "imperfect3d" => imperfect3d(nodes)?
    else
      error
    end

actor Gossip
  let _main: Main tag

  new create(main: Main tag) =>
    _main = main

  be run(total_nodes: USize, topology: String) =>
    let nodes = Array[NodeLike tag]
    var i: USize = 0
    while i < total_nodes do
      nodes.push(Node(_main, i.string()))
      i = i + 1
    end

    try
      TopologyFunctions[NodeLike tag].create_topology(nodes, topology)?
      _main.out("Topology created: " + topology)
      nodes(0)?.receive_message("The rumor")
    else
      _main.out("Error creating topology: " + topology)
    end

actor Node is NodeLike
  let _main: Main tag
  let _id: String
  let _neighbors: Array[Node tag]
  var _rumor_count: USize = 0
  let _timers: Timers = Timers
  var _active: Bool = true

  new create(main: Main tag, id: String) =>
    _main = main
    _id = id
    _neighbors = Array[Node tag]

  be add_neighbor(neighbor: NodeLike tag) =>
    match neighbor
    | let n: Node tag => _neighbors.push(n)
    end

  be start() =>
    None

  be receive_message(msg: (String | (F64, F64))) =>
    match msg
    | let rumor: String => receive_rumor(rumor)
    | (let _: F64, let _: F64) =>
      None
    end

  be receive_rumor(rumor: String) =>
    if _rumor_count < 10 then
      _rumor_count = _rumor_count + 1
      if _rumor_count == 1 then
        _main.node_informed(_id)
      elseif _rumor_count == 10 then
        _main.node_stopped(_id)
      end
    end
    if _active then
      _timers(Timer(SpreadNotify(this), 0, 10_000_000))
    end

  be spread_rumor() =>
    if _active then
      for neighbor in _neighbors.values() do
        neighbor.receive_rumor("The rumor")
      end
    end

  be stop_spreading() =>
    _active = false
    _timers.dispose()

  be debug_info() =>
    _main.out("Node " + _id + " has " + _neighbors.size().string() + " neighbors")

class SpreadNotify is TimerNotify
  let _node: Node tag

  new iso create(node: Node tag) =>
    _node = node

  fun ref apply(timer: Timer, count: U64): Bool =>
    _node.spread_rumor()
    true

  fun ref cancel(timer: Timer) =>
    None

actor PushSum
  let _main: Main tag

  new create(main: Main tag) =>
    _main = main

  be run(total_nodes: USize, topology: String) =>
    let nodes = Array[NodeLike tag]
    var i: USize = 0
    while i < total_nodes do
      nodes.push(PushSumNode(_main, i.string()))
      i = i + 1
    end

    try
      TopologyFunctions[NodeLike tag].create_topology(nodes, topology)?
      _main.out("Topology created: " + topology)
      nodes(0)?.start()
    else
      _main.out("Error creating topology: " + topology)
    end

actor PushSumNode is NodeLike
  let _main: Main tag
  let _id: String
  var _s: F64
  var _w: F64
  var _old_ratio: F64
  var _rounds_unchanged: USize
  var _rounds: USize
  let _neighbors: Array[PushSumNode tag]
  let _timers: Timers = Timers
  var _active: Bool = true
  var _terminated: Bool = false

  new create(main: Main tag, id: String) =>
    _main = main
    _id = id
    _s = try
      id.f64()?
    else
      0.0
    end
    _w = 1.0
    _old_ratio = 0.0
    _rounds = 0
    _rounds_unchanged = 0
    _neighbors = Array[PushSumNode tag]

  be add_neighbor(neighbor: NodeLike tag) =>
    match neighbor
    | let n: PushSumNode tag => _neighbors.push(n)
    end

  be start() =>
    if _active then
      _send_to_random_neighbor()
      _timers(Timer(PushSumNotify(this), 0, 10_000_000))
    end

  be receive_message(msg: (String | (F64, F64))) =>
    match msg
    | (let s_received: F64, let w_received: F64) =>
      if _active then
        _s = _s + s_received
        _w = _w + w_received
        _send_to_random_neighbor()
      end
    | let _: String =>
      None
    end

  be receive_rumor(rumor: String)=>
    None

  be _send_to_random_neighbor() =>
    if (_neighbors.size() > 0) and (_w > 0) then
      let s_to_send = _s / 2
      let w_to_send = _w / 2
      _s = _s / 2
      _w = _w / 2
      try
        let rand = Rand(Time.nanos())
        let index = rand.int(_neighbors.size().u64()).usize()
        _neighbors(index)?.receive_message((s_to_send, w_to_send))
      end
    end
    _timers(Timer(PushSumNotify(this), 100_000_000))

  be check_termination() =>
    if _active then
      let current_ratio = _s / _w
      if (current_ratio - _old_ratio).abs() < 1e-10 then
        _rounds_unchanged = _rounds_unchanged + 1
        if _rounds_unchanged >= 3 then
          _terminate()
        else
          _send_to_random_neighbor()
        end
      else
        _rounds_unchanged = 0
        _send_to_random_neighbor()
      end
      _old_ratio = current_ratio
    end

  be _terminate() =>
    if not _terminated then
      _active = false
      _terminated = true
      _timers.dispose()
      _main.node_terminated(_id)
    end

class PushSumNotify is TimerNotify
  let _node: PushSumNode tag

  new iso create(node: PushSumNode tag) =>
    _node = node

  fun ref apply(timer: Timer, count: U64): Bool =>
    _node.check_termination()
    true

  fun ref cancel(timer: Timer) =>
    None