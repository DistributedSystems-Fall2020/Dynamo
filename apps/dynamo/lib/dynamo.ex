defmodule Dynamo do
  @moduledoc """
  An implementation of the Raft consensus protocol.
  """
  # Shouldn't need to spawn anything from this module, but if you do
  # you should add spawn to the imports.
  import Emulation, only: [send: 2, timer: 1, now: 0, whoami: 0, cancel_timer: 1]

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
    pending_get_req: nil,
    pending_get_rsp: nil, 
    failed_nodes: nil, 
    seed_node: nil, 
    node_list: nil
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
      node_list: nodes
    }
  end 

  # @spec get_preference_list_helper(any(), ) :: list() '
  
  defp get_preference_helper(curr_count, curr_index, preference_list, node_set ,count, initial_index, node_list, first ) do 
    if curr_count == count or (curr_index == initial_index and not first) do 
      # @TODO : fix this count bit to see 
      preference_list 
    else 
      if(not MapSet.member?(node_set, Enum.at(node_list, curr_index)) ) do 
        preference_list = preference_list ++ [Enum.at(node_list, curr_index)] 
        curr_count = curr_count + 1 
        node_set = MapSet.put(node_set, Enum.at(node_list, curr_index) )
        curr_index = if curr_index == length(node_list) - 1 do 0 else curr_index + 1 end
        get_preference_helper(curr_count, curr_index, preference_list, node_set ,count, initial_index, node_list, false)
      else 
        curr_index = if curr_index == length(node_list) - 1 do 0 else curr_index + 1 end
        get_preference_helper(curr_count, curr_index, preference_list, node_set ,count, initial_index, node_list, false)
      end 
    end 


  end



  @spec get_preference_list(any(), string(), non_neg_integer()) :: list() 
  defp get_preference_list(nodeList, key, count) do 
    hash_list = Enum.map(nodeList, fn [hash| _] -> hash end)
    node_list = Enum.map(nodeList, fn [_| node] -> node end)
    initial_node = Bisect.bisect_left(hash_list, Utils.hash(key))

    get_preference_helper(0, initial_node, [], MapSet.new() ,count, initial_node, node_list, true )

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
    state = %{state |  local_store: %{}, pending_put_req: %{}, pending_put_rsp: %{}, pending_get_req: %{}, pending_get_rsp: %{}, failed_nodes: MapSet.new()}
    state = add_nodes_to_ring(state, 0)
    server(state, nil)
  end


  def server(state, extra_state) do 
    receive do
      {sender,  %Dynamo.Client.GetMessage{
                  key: key,
                  metadata: metadata, 
                }} ->  

        IO.puts("get message works")  
        msg  = retrieve(state, key)   
        IO.puts("Retrieved the message #{inspect(msg)}")     
        server(state, nil)
      {sender,  %Dynamo.Client.PutMessage{
            key: key,
            value: value,
            metadata: metadata
          }} -> 

        IO.puts("put message works")
        state = store(state, key, value, metadata)
        IO.puts("Put #{inspect(state.local_store)}")
        server(state, nil)
      {sender,  %Dynamo.GetMessage{
          key: key,
          metadata: metadata, 
        }} 

      {sender,  %Dynamo.GetMessage{
          key: key,
          metadata: metadata, 
        }} 
      
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
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  alias __MODULE__ 
  @enforce_keys [:server]
  defstruct(server: nil, local_store: nil) 
  
  @spec new_client(atom()) :: %Client{server: atom()}
  def new_client(member) do 
    IO.puts("CREATING A CLIENT")
    %Client{server: member, 
            local_store: %{}}
  end 

  @spec put(%Client{}, string(), map() , non_neg_integer(), atom()) :: boolean()
  def put(client, key, metadata, value, server) do 
    # client sends a request, waits for a time .
    server = client.server
    # if map[key] do 
    #   send(server, {:put, {key, {value, local_store[key]}}})
    # else 
    #   send(server, {:put, {key, {value, }}})
    send(server, %Dynamo.Client.PutMessage{
                    key: key, 
                    metadata: metadata,
                    value: value
                  }
                )

  end

  @spec get(%Client{}, string(), map(), atom()) :: boolean()
  def get(client, key, metadata, server) do 
    #the test case generates a random server. This way we have more control over which 
    #server need to send it to. This helps in testing the redirection 
    server = client.server
    send(server, %Dynamo.Client.GetMessage{
                    key: key, 
                    metadata: metadata
                  }
                )
  end

end



