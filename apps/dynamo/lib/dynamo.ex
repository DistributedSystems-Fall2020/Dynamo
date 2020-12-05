defmodule Dynamo do
  @moduledoc """
  An implementation of the Raft consensus protocol.
  """
  # Shouldn't need to spawn anything from this module, but if you do
  # you should add spawn to the imports.
  import Emulation, only: [send: 2, timer: 1, now: 0, whoami: 0, cancel_timer: 1, spawn: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  # This allows you to use Elixir's loggers
  # for messages. See
  # https://timber.io/blog/the-ultimate-guide-to-logging-in-elixir/
  # if you are interested in this. Note we currently purge all logs
  # below Info
  require Logger
  alias ExHashRing.HashRing
  alias ExHashRing.HashRing.Utils

  defstruct(
    num_virtual_nodes: nil,  # Number of repeats for nodes in consistent hash table
    num_replicas: nil , # Number of nodes to replicate at
    num_writes: nil , # Number of nodes that need to reply to a write operation
    num_reads: nil, # Number of nodes that need to reply to a read operation
    local_store: nil, 
    ring: nil,
    pending_put_req: nil,
    pending_put_rsp: nil,
    pending_put_msg: nil,
    pending_get_req: nil,
    pending_get_rsp: nil, 
    pending_get_msg: nil,
    failed_nodes: nil, 
    seed_node: nil, 
    node_list: nil, 
    seq_no: nil,
    msg_timers: nil
  )


  @spec new_configuration(
    non_neg_integer(), 
    non_neg_integer(), 
    non_neg_integer(), 
    non_neg_integer(), 
    list()
  ) :: %Dynamo{}
  def new_configuration(t, n, w, r, nodes) do
    ring = HashRing.new([], t)
    %Dynamo{
      num_virtual_nodes: t, 
      num_replicas: n, 
      num_writes: w, 
      num_reads: r,
      ring: ring, 
      node_list: nodes, 
      seq_no: 0,
      msg_timers: %{}
    }
  end 

  # @spec get_preference_list_helper(any(), ) :: list() '
  
  defp get_preference_helper(curr_count, curr_index, preference_list, node_set ,count, initial_index, node_list, avoid , avoided,first ) do 
    if curr_count == count or (curr_index == initial_index and not first) do 
      {preference_list, avoided} 
    else 
      if(not MapSet.member?(node_set, Enum.at(node_list, curr_index)) and not MapSet.member?(avoid, Enum.at(node_list, curr_index))) do 
        preference_list = preference_list ++ [Enum.at(node_list, curr_index)]
        curr_count = curr_count + 1 
        node_set = MapSet.put(node_set, Enum.at(node_list, curr_index) )
        curr_index = if curr_index == length(node_list) - 1 do 0 else curr_index + 1 end
        get_preference_helper(curr_count, curr_index, preference_list, node_set ,count, initial_index, node_list, avoid, avoided,false)
      else 
        avoided = if MapSet.member?(avoid, Enum.at(node_list, curr_index)) do MapSet.put(avoided, Enum.at(node_list, curr_index) ) else avoided end
        # IO.puts("AVOIDED #{inspect(Enum.at(node_list, curr_index))} #{inspect(avoid)} #{inspect(MapSet.member?(avoid, :b))}")
        
        curr_index = if curr_index == length(node_list) - 1 do 0 else curr_index + 1 end
        get_preference_helper(curr_count, curr_index, preference_list, node_set ,count, initial_index, node_list, avoid, avoided ,false)
      end 
    end 
  end



  @spec get_preference_list(any(), string(), non_neg_integer(), any()) :: list() 
  defp get_preference_list(ring, key, count, failed_nodes) do 
    nodeList = PhStTransform.transform(ring.items, %{Tuple => fn(tuple) -> Tuple.to_list(tuple) end})

    hash_list = Enum.map(nodeList, fn [hash| _] -> hash end)
    node_list = Enum.map(nodeList, fn [_| node] -> node end)
    node_list = List.flatten(node_list)
    # IO.puts("nodelist #{inspect(node_list)}")
    initial_node = Bisect.bisect_left(hash_list, Utils.hash(key))
   
    get_preference_helper(0, initial_node, [], MapSet.new() ,count, initial_node, node_list, failed_nodes, MapSet.new(), true )

  end 

  @spec add_nodes_to_ring(map(), non_neg_integer()) :: map()
  defp  add_nodes_to_ring(state, pos) do
    if pos == length(state.node_list) do
      state
    else
      node = Enum.at(state.node_list, pos)
      ring = state.ring 
      {:ok, ring} = HashRing.add_node(state.ring, node)
      add_nodes_to_ring(%{state | ring: ring}, pos+1) 
    end
  end

  @spec become_server(%Dynamo{}) :: no_return() 
  def become_server(state) do 
    {:ok, ring} = HashRing.add_node(state.ring, whoami())
    state = %{state |  local_store: %{}, pending_put_req: %{}, pending_put_rsp: %{}, pending_put_msg: %{},
      pending_get_req: %{}, pending_get_rsp: %{}, pending_get_msg: %{}, failed_nodes: MapSet.new()}
    state = add_nodes_to_ring(state, 0)
    server(state, nil)
  end

  @spec send_to_all_members(list(), map(), non_neg_integer(), any()) :: map()
  defp send_to_all_members(preference_list, state, seq_no, msg) do 
    if preference_list != [] do
      me = whoami()
      [head | tail] = preference_list
      if head != me do
        send(head, msg)
        cond do
        Map.has_key?(state.pending_get_req, seq_no) ->
          IO.puts("Calling GET Send #{inspect(me)} #{inspect(seq_no)}")
          {:ok, pending_get_req_seq_no} = Map.fetch(state.pending_get_req, seq_no)
          pending_get_req_seq_no = MapSet.put(pending_get_req_seq_no, head)
          pending_get_req = Map.put(state.pending_get_req, seq_no, pending_get_req_seq_no)
          timer = Emulation.timer(50, {:timer, me, head, seq_no})
          IO.puts("Inserting timer for GET #{inspect(me)} #{inspect(head)} #{inspect(seq_no)} #{inspect(timer)}")
          msg_timers = Map.put(state.msg_timers, {:timer, me, head, seq_no}, timer)
          state = %{state | msg_timers: msg_timers, pending_get_req: pending_get_req}
          if tail != [] do
            send_to_all_members(tail, state, seq_no, msg)
          else
            # me = whoami()
            # {:ok, pending_get_req_seq_no} = Map.fetch(state.pending_get_req, seq_no)
            # pending_get_req_seq_no = MapSet.put(pending_get_req_seq_no, me)
            # pending_get_req = Map.put(state.pending_get_req, seq_no, pending_get_req_seq_no)
            # state = %{state | pending_get_req: pending_get_req}
            state
          end
        Map.has_key?(state.pending_put_req, seq_no) ->
          IO.puts("Calling PUT Send #{inspect(me)} #{inspect(seq_no)}")
          {:ok, pending_put_req_seq_no} = Map.fetch(state.pending_put_req, seq_no)
          pending_put_req_seq_no = MapSet.put(pending_put_req_seq_no, head)
          pending_put_req = Map.put(state.pending_put_req, seq_no, pending_put_req_seq_no)
          timer = Emulation.timer(50, {:timer, me, head, seq_no})
          IO.puts("Inserting timer for PUT #{inspect(me)} #{inspect(head)} #{inspect(seq_no)} #{inspect(timer)}")
          msg_timers = Map.put(state.msg_timers, {:timer, me, head, seq_no}, timer)
          state = %{state | msg_timers: msg_timers, pending_put_req: pending_put_req}
          if tail != [] do
            send_to_all_members(tail, state, seq_no, msg)
          else
            # me = whoami()
            # {:ok, pending_put_req_seq_no} = Map.fetch(state.pending_put_req, seq_no)
            # pending_put_req_seq_no = MapSet.put(pending_put_req_seq_no, me)
            # pending_put_req = Map.put(state.pending_put_req, seq_no, pending_put_req_seq_no)
            # state = %{state | pending_put_req: pending_put_req}
            state
          end
        true -> IO.puts("Calling nobody")
        end
      else
        send_to_all_members(tail, state, seq_no, msg)
      end
    else
      state
    end
  end 

  @spec send_to_first_members(list(), map(), map(), non_neg_integer(), any()) :: map()
  defp send_to_first_members(preference_list, state, msg_req, seq_no, msg) do 
    if preference_list != [] do
      me = whoami()
      [head | tail] = preference_list
      if head != me do
        {:ok, msg_seq} = Map.fetch(msg_req, seq_no)
        if MapSet.member?(msg_seq, head) do 
          send_to_first_members(tail, state, msg_req, seq_no, msg)
        else
          send(head, msg)
          timer = Emulation.timer(10, {:timer, me, head, seq_no})
          IO.puts("Inserting timer for #{inspect(me)} #{inspect(head)} #{inspect(seq_no)} #{inspect(timer)}")
          msg_timers = Map.put(state.msg_timers, {:timer, me, head, seq_no}, timer)
          state = %{state | msg_timers: msg_timers}
          msg_seq = MapSet.put(msg_seq, head)
          msg_req = Map.put(msg_req, seq_no, msg_seq)
          {:ok, state, msg_req}
        end
      else
        send_to_first_members(tail, state, msg_req, seq_no, msg)
      end
    else
      {:error, state, msg_req}
    end
  end 

  @spec get_next_seq_no(map()) :: non_neg_integer()
  defp get_next_seq_no(state) do
    seq_no = state.seq_no+1
  end

  defp check_get_satisfied(server_resp, count, client_resp) do
    cond do
      count == 0 -> {true, client_resp}
      length(server_resp) < count -> {false, []}
      true ->
        [head|tail] = server_resp
        {server, value, metadata} = head
        if MapSet.member?(MapSet.new(client_resp), {value, metadata}) do
          check_get_satisfied(tail, count-1, client_resp)
        else
          client_resp = client_resp ++ [{value, metadata}]
          check_get_satisfied(tail, count-1, client_resp)
        end
    end
  end

  def server(state, extra_state) do 
    me = whoami()
    receive do
      {sender,  %Dynamo.Client.GetMessage{
                  key: key,
                  metadata: metadata, 
                  client: client
                }} ->  
        
        {preference_list, avoided} =  get_preference_list(state.ring, key, state.num_replicas, state.failed_nodes)
        if not Enum.member?(preference_list, whoami()) and length(preference_list) >= 1 do   
          send(Enum.at(preference_list, 0), %Dynamo.Client.GetMessage{
            key: key,
            metadata: metadata, 
            client: client
          })
          server(state, nil)
        end 

        seq_no = get_next_seq_no(state)
        pending_get_req = Map.put(state.pending_get_req, seq_no, MapSet.new())
        state = %{state | seq_no: seq_no, pending_get_req: pending_get_req}
        state = send_to_all_members(preference_list, state, seq_no, %Dynamo.GetRequest{
          key: key, 
          metadata: metadata, 
          seq_no: seq_no,
          handoff: nil})

        me = whoami()
        {:ok, pending_get_req_seq_no} = Map.fetch(state.pending_get_req, seq_no)
        pending_get_req_seq_no = MapSet.put(pending_get_req_seq_no, me)
        pending_get_req = Map.put(state.pending_get_req, seq_no, pending_get_req_seq_no)
        state = %{state | pending_get_req: pending_get_req}

        IO.puts("Who got the GET request? #{inspect(me)} #{inspect(state.pending_get_req)}")

        # IO.puts("The preference list is #{inspect(preference_list)}")
        IO.puts("get message works: #{inspect(whoami())}")  
        {value, metadata} = retrieve(state, key)
        pending_get_rsp = Map.put(state.pending_get_rsp, seq_no, MapSet.new([{me, value, metadata}]))
        pending_get_msg = Map.put(state.pending_get_msg, seq_no, %Dynamo.Client.GetMessage{
          key: key,
          metadata: metadata, 
          client: client
        })
        # IO.puts("Pending get: #{inspect(pending_get_rsp)}")
        state = %{state | pending_get_rsp: pending_get_rsp, pending_get_msg: pending_get_msg}
        # IO.puts("Retrieved the message #{inspect(msg)}")     
        server(state, nil)

      {sender,  %Dynamo.Client.PutMessage{
            key: key,
            value: value,
            metadata: metadata,
            client: client
          }} -> 
        {preference_list, avoided} =  get_preference_list(state.ring, key, state.num_replicas+3, state.failed_nodes)
        if not Enum.member?(preference_list, whoami()) and length(preference_list) >= 1 do   
          send(Enum.at(preference_list, 0), %Dynamo.Client.PutMessage{
            key: key,
            value: value,
            metadata: metadata, 
            client: client
          })
          server(state, nil)
        end 

        seq_no = get_next_seq_no(state)
        metadata = Dynamo.VectorClock.update_vector_clock(metadata, whoami(), seq_no) 
        pending_put_req = Map.put(state.pending_put_req, seq_no, MapSet.new())
        state = %{state | seq_no: seq_no, pending_put_req: pending_put_req}
        state = send_to_all_members(preference_list, state, seq_no, %Dynamo.PutRequest{
          key: key, 
          value: value,
          metadata: metadata, 
          seq_no: seq_no,
          handoff: nil})

        me = whoami()
        {:ok, pending_put_req_seq_no} = Map.fetch(state.pending_put_req, seq_no)
        pending_put_req_seq_no = MapSet.put(pending_put_req_seq_no, me)
        pending_put_req = Map.put(state.pending_put_req, seq_no, pending_put_req_seq_no)
        state = %{state | pending_put_req: pending_put_req}

        IO.puts("Who got the PUT request? #{inspect(state.pending_put_req)}")

        IO.puts("put message works #{inspect(whoami())}")
        state = store(state, key, value, metadata)
        pending_put_rsp = Map.put(state.pending_put_rsp, seq_no, MapSet.new([me]))
        pending_put_msg = Map.put(state.pending_put_msg, seq_no, %Dynamo.Client.PutMessage{
          key: key,
          value: value,
          metadata: metadata, 
          client: client
        })
        state = %{state | pending_put_rsp: pending_put_rsp, pending_put_msg: pending_put_msg}
        server(state, nil)

      {sender,  %Dynamo.GetRequest{
          key: key,
          metadata: metadata, 
          seq_no: seq_no,
          handoff: handoff
        }} -> 
        me = whoami()
        if me == :c do
          server(state, nil)
        end
        IO.puts("Server received get request")
        {value, metadata} = retrieve(state, key)
        send(sender, %Dynamo.GetResponse{
          key: key,
          value: value,
          metadata: metadata, 
          seq_no: seq_no
        })
        server(state, nil)

      {sender,  %Dynamo.PutRequest{
          key: key,
          value: value,
          metadata: metadata, 
          seq_no: seq_no,
          handoff: handoff
        }} -> 
        me = whoami()
        if me == :c do
          server(state, nil)
        end
        IO.puts("Server received put request: #{inspect(me)}")
        state = store(state, key, value, metadata)
        if handoff != nil do
          IO.puts("I got a handoff: #{inspect(me)} #{inspect(handoff)}")
        end
        send(sender, %Dynamo.PutResponse{
          key: key,
          value: value,
          metadata: metadata, 
          status: :ok,
          seq_no: seq_no
        })
        server(state, nil)

      {sender,  %Dynamo.GetResponse{
          key: key,
          value: value,
          metadata: metadata, 
          seq_no: seq_no
        }} -> 
        IO.puts("Server received get response")
        if Map.has_key?(state.msg_timers, {:timer, me, sender, seq_no}) do
          {:ok, timer} = Map.fetch(state.msg_timers, {:timer, me, sender, seq_no})
          k = Emulation.cancel_timer(timer)
          msg_timers = Map.delete(state.msg_timers, {:timer, me, sender, seq_no})
          state = %{state | msg_timers: msg_timers}
          if value == nil do 
            server(state, nil)
          end
          {:ok, mpset_seq_no} = Map.fetch(state.pending_get_rsp, seq_no)
          mpset_seq_no = MapSet.put(mpset_seq_no, {sender, value, metadata})
          state = %{state | pending_get_rsp: Map.put(state.pending_get_rsp, seq_no, mpset_seq_no)}
          {is_enough, responses} = check_get_satisfied(MapSet.to_list(state.pending_get_rsp[seq_no]), state.num_writes, [])
          if is_enough do 
            #coalesce of clocks
            result = Dynamo.VectorClock.coalesce2(responses)
            {:ok, msg} = Map.fetch(state.pending_get_msg, seq_no)
            send(msg.client, {key, result})
            server(state, nil)
          end
          server(state, nil)
        end
        server(state, nil)

      {sender,  %Dynamo.PutResponse{
          key: key,
          value: value,
          metadata: metadata, 
          status: status,
          seq_no: seq_no
        }} -> 
        IO.puts("Server received put response #{inspect(whoami())} #{inspect(sender)}")
        if Map.has_key?(state.msg_timers, {:timer, me, sender, seq_no}) do
          {:ok, timer} = Map.fetch(state.msg_timers, {:timer, me, sender, seq_no})
          k = Emulation.cancel_timer(timer)
          IO.puts("PUT timer cancelled: #{inspect(me)} #{inspect(sender)} #{inspect(k)}")
          msg_timers = Map.delete(state.msg_timers, {:timer, me, sender, seq_no})
          state = %{state | msg_timers: msg_timers}
          if status == :ok do
            {:ok, mpset_seq_no} = Map.fetch(state.pending_put_rsp, seq_no)
            mpset_seq_no = MapSet.put(mpset_seq_no, sender)
            state = %{state | pending_put_rsp: Map.put(state.pending_put_rsp, seq_no, mpset_seq_no)}
            if MapSet.size(state.pending_put_rsp[seq_no]) >= state.num_reads do 
              {:ok, msg} = Map.fetch(state.pending_put_msg, seq_no)
              send(msg.client, {:ok, key})
              server(state, nil)
            end
            server(state, nil)
          end
          server(state, nil)
        end
        server(state, nil)

      {:timer, me, sender, seq_no} -> 
        IO.puts("timer went off: #{inspect(me)} #{inspect(sender)} #{inspect(seq_no)}")
        {:ok, timer} = Map.fetch(state.msg_timers, {:timer, me, sender, seq_no})
        Emulation.cancel_timer(timer)
        msg_timers = Map.delete(state.msg_timers, {:timer, me, sender, seq_no})
        state = %{state | msg_timers: msg_timers}
        cond do
          Map.has_key?(state.pending_put_msg, seq_no) -> 
            IO.puts("Timer for PUT seq_no: #{inspect(seq_no)}")
            failed_nodes = MapSet.put(state.failed_nodes, sender)
            state = %{state | failed_nodes: failed_nodes}
            {:ok, put_msg} = Map.fetch(state.pending_put_msg, seq_no)
            {preference_list, avoided} =  get_preference_list(state.ring, put_msg.key, state.num_replicas+3, state.failed_nodes)
            IO.inspect(preference_list)
            send_msg = %Dynamo.PutRequest{
              key: put_msg.key, 
              value: put_msg.value,
              metadata: put_msg.metadata, 
              seq_no: seq_no,
              handoff: sender
            }
            {status, state, pending_put_req} = send_to_first_members(preference_list, state, state.pending_put_req, seq_no, send_msg)
            state = %{state | pending_put_req: pending_put_req}
            IO.puts("Preference list after PUT timer: #{inspect(preference_list)} #{inspect(state.pending_put_req)}")
            server(state, nil)

          Map.has_key?(state.pending_get_msg, seq_no) -> 
            IO.puts("Timer for GET seq_no: #{inspect(seq_no)}")
            IO.puts("GET SENT before timer #{inspect(state.pending_get_req)}")
            failed_nodes = MapSet.put(state.failed_nodes, sender)
            {:ok, get_msg} = Map.fetch(state.pending_get_msg, seq_no)
            {preference_list, avoided} =  get_preference_list(state.ring, get_msg.key, state.num_replicas, state.failed_nodes)
            send_msg = %Dynamo.GetRequest{
              key: get_msg.key, 
              metadata: get_msg.metadata, 
              seq_no: seq_no,
              handoff: sender
            }
            {status, state, pending_get_req} = send_to_first_members(preference_list, state, state.pending_get_req, seq_no, send_msg)
            state = %{state | pending_get_req: pending_get_req}
            IO.puts("Preference list after GET timer: #{inspect(preference_list)} #{inspect(state.pending_get_req)}")
            server(state, nil)
          true -> IO.puts("Timer for empty")
            server(state, nil)
        end


        server(state, nil)

    end
    

  end


  defp print_st(state) do 
    IO.puts("----- Printing state ----")
    IO.puts("#{inspect(state)}")
  end 
  
  defp store(state, key, value, metadata) do
    local_store = Map.put(state.local_store, key, {value, metadata})
    %{state | local_store: local_store}
  end

  defp retrieve(state, key) do 
    local_store = state.local_store 
    if Map.has_key?(local_store, key) do 
      Map.get(local_store, key) 
    else 
      {nil,nil}
    end
  end

  @spec test_add_to_ring() :: no_return()
  def test_add_to_ring() do

    state = new_configuration(10,3,2,2, [:b, :c])
    state = add_nodes_to_ring(state, 0)

    IO.puts("Testing store: #{inspect(state.ring)}")
  end

  @spec test_store() :: no_return()
  def test_store() do
    key = "ag"
    value = 1
    metadata = %{}
    state = new_configuration(10,3,2,2, [:b, :c])
    state = %{state | local_store: %{}}
    state = store(state, key, value, metadata)
    IO.puts("Testing store: #{inspect(state)}")
  end

  @spec test_retrieve() :: no_return()
  def test_retrieve() do
    key = "ag"
    value = 1
    metadata = %{}
    state = new_configuration(10,3,2,2, [:b, :c])
    state = %{state | local_store: %{}}
    state = store(state, key, value, metadata)
    value = retrieve(state, key) 
    IO.puts("Testing retrieve: #{inspect(value)}")
  end
end

defmodule Dynamo.Client do
  import Emulation, only: [send: 2, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  alias __MODULE__
  @enforce_keys [:node_list]
  defstruct(node_list: nil, test_client: nil)
    @spec new_client(atom()) :: %Dynamo.Client{node_list: atom()}
  def new_client(node_list) do
    IO.puts("CREATING A CLIENT")
    %Dynamo.Client{node_list: node_list,
            test_client: nil
          }
  end

  @spec client(%Dynamo.Client{}) :: no_return() 
  def client(state) do 
    receive do 
      {sender, %Dynamo.ToClientPutMessage{
        key: key, 
        value: value, 
        metadata: metadata, 
        node_list: node_list
      }} -> 
        me = whoami()
        node_list = if node_list == nil do state.node_list else node_list end
    
        server =  Enum.random(node_list)
    
        metadata = if map_size(metadata) == 0  do %Dynamo.VectorClock{} else Dynamo.VectorClock.converge(metadata) end
        send(server, %Dynamo.Client.PutMessage{
                        key: key,
                        metadata: metadata,
                        value: value,
                        client: me
                      })

        client(%{state| test_client: sender})
      {sender, %Dynamo.ToClientGetMessage{
        key: key, 
        metadata: metadata, 
        node_list: node_list
      }} ->
        me = whoami()
        node_list = if node_list == nil do state.node_list else node_list end
        server =  Enum.random(node_list)
        send(server, %Dynamo.Client.GetMessage{
                        key: key,
                        metadata: metadata,
                        client: me
                      }
                    )
        client(%{state| test_client: sender})
      
      {sender, {key, responses}} -> 
        send(state.test_client, {:get, key, responses})
        client(state)

      {sender, {:ok, key}} -> 
        send(state.test_client, {:put, :ok, key})
        client(state)
    end 
  end


#   @spec put(%Dynamo.Client{}, string(), map() , non_neg_integer(), atom()) :: boolean()
#   def put(client, key, metadata, value, server, node_list \\ nil) do
#     me = whoami()
#     node_list = if node_list == nil do client.node_list else node_list end

#     server =  Enum.random(node_list)

#     metadata = if map_size(metadata) == 0  do %Dynamo.VectorClock{} else Dynamo.VectorClock.converge(metadata) end
#     send(server, %Dynamo.Client.PutMessage{
#                     key: key,
#                     metadata: metadata,
#                     value: value,
#                     client: me
#                   }
#                 )

#   end

#   @spec get(%Dynamo.Client{}, string(), map(), atom()) :: boolean()
#   def get(client, key, metadata, server, node_list \\ nil) do
#     me = whoami()
#     node_list = if node_list == nil do client.node_list else node_list end
#     server =  Enum.random(node_list)
#     send(server, %Dynamo.Client.GetMessage{
#                     key: key,
#                     metadata: metadata,
#                     client: me
#                   }
#                 )
#   end

# end




