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
    ring: nil 
  )


  @spec new_configuration(
    non_neg_integer(), 
    non_neg_integer(), 
    non_neg_integer(), 
    non_neg_integer()
  ) :: %Dynamo{}
  def new_configuration(t, n, w, r) do
    %Dynamo{
      num_virtual_nodes: t, 
      num_replicas: n, 
      num_writes: w, 
      num_reads: r, 
      ring: HashRing.new([], n)
    }
  end 

  # @spec get_preference_list_helper(any(), ) :: list() '
  
  defp get_preference_helper(curr_count, curr_index, preference_list, node_set ,count, initial_index, node_list, first ) do 
    if curr_count == count or (curr_index == initial_index and not first) do 
      # @TODO : fix this count bit to see 
      preference_list 
    else 
      # # Enum.at(node_list, curr_index) 
      # preference_list = preference_list ++ [Enum.at(node_list, curr_index) ] 
      # IO.puts("SECOND #{inspect(preference_list)}")
      # curr_count = curr_count + 1 
      # get_preference_helper(curr_count, curr_index, preference_list, node_set ,count, initial_index, node_list)


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
    # nodes = PhStTransform.transform(nodeList, %{Tuple => fn(tuple) -> Tuple.to_list(tuple) end})
    hash_list = Enum.map(nodeList, fn [hash| _] -> hash end)
    node_list = Enum.map(nodeList, fn [_| node] -> node end)
    initial_node = Bisect.bisect_left(hash_list, Utils.hash(key))

    final = get_preference_helper(0, initial_node, [], MapSet.new() ,count, initial_node, node_list, true )
    IO.puts("Preference List: #{inspect(final)}")
    # IO.puts("#{inspect(node_list)}")
    # IO.puts("#{inspect(Utils.hash(key))}")

    # IO.puts("#{initial_node}")

    

    # # IO.puts("#{inspect(nodeList)}")
    # # IO.puts("#{inspect(Utils.hash(key))}")

    # a = PhStTransform.transform(nodeList, %{Tuple => fn(tuple) -> Tuple.to_list(tuple) end})

    # # {hashlist, nodes } = Enum.unzip(a)
    # hashlist = Enum.map(a, fn [hash| _] -> hash end)

    # # second = Enum.at(hashlist, 3)
    # IO.puts("#{inspect(second)}")

    # # IO.puts("#{inspect(nodeList)}")
    # # hashlist = [] 
    # # hashlist = [0 | hashlist]
    # # Enum.each(a, fn [h|t] -> h end)

  
    # IO.puts("Hashlist #{inspect(hashlist)}")
    # # search(hashlist, fn x -> x > Utils.hash(key) end)
    # IO.puts("Bisec #{inspect(Bisect.bisect_left([2,5],4))}")

    #   #     iex> Bisect.search([1, 2, 4, 8], fn x ->
    #   # ...>   x == 7
    #   # ...> end)

    # # for h in a 
    # #  IO.puts("#{inspect(a)}")

  end 


  @spec become_server(%Dynamo{}) :: no_return() 
  def become_server(state) do 
    IO.puts("hi")
    ring = HashRing.new([], 2)
    {:ok, ring} = HashRing.add_node(ring, "a")
    {:ok, ring} = HashRing.add_node(ring, "b")
    {:ok, ring} = HashRing.add_node(ring, "c")
    {:ok, ring} = HashRing.add_node(ring, "d")
    {:ok, ring} = HashRing.add_node(ring, "e")
    nodes = HashRing.find_node(ring, "ag")
    IO.puts("#{inspect(ring.items)}")
    ringList =  PhStTransform.transform(ring.items, %{Tuple => fn(tuple) -> Tuple.to_list(tuple) end})
    get_preference_list(ringList, "ag" , state.num_replicas)

    # IO.puts("UTILS!! #{inspect(Utils.hash("key20"))}")
    # IO.puts(" #{inspect(nodes)}")

    # IO.puts("#{inspect(ring.items)}")
    # IO.puts("#{inspect(ring2)}")
    # print_st(state)
  end

  defp print_st(state) do 
    IO.puts("hi")

    IO.puts("#{inspect(state)}")
  end 
  
  # @spec reliable_kv_server(map(), number()) :: no_return()
  # defp reliable_kv_server(state) do
  #   receive do
  #     {sender, {@get, key}} ->
  #       # TODO: Send a message `{key, current value(key)}`
  #       # to the sender using `reliable_send`.
  #       # If the key is not currently present send
  #       # `{key, nil}`. You might find
  #       # https://hexdocs.pm/elixir/Map.html
  #       # useful.
  #       # You should use count as the nonce for
  #       # reliable_send, and update it each time you
  #       # send a message.
  #       value = Map.get(state, key)
  #       message = {key, value}
  #       send(sender, message)
  #       reliable_kv_server(state)

  #     {_sender, {@set, key, {value, context}}} ->
  #       # TODO: Store  value for the given key
  #       # in the state. You should not send any
  #       # message to the sender. You might find
  #       # https://hexdocs.pm/elixir/Map.html
  #       # useful.
  #       state = Map.put(state, key, {value, context})
  #       reliable_kv_server(state)
  #   end
  # end

  # @spec test_kv_client(atom(), pid()) :: boolean()
  # defp test_kv_client(server, caller) do
  #   reliable_send(server, {@set, :a, 1}, 1, @send_timeout)
  #   reliable_send(server, {@set, :b, 22}, 2, @send_timeout)
  #   reliable_send(server, {@get, :a}, 3, @send_timeout)

  #   case reliable_receive() do
  #     {^server, m} ->
  #       send(caller, m == {:a, 1})
  #       m == {:a, 1}

  #     _ ->
  #       send(caller, false)
  #       false
  #   end
  # end

  # @doc """
  # Test reliable key value server.
  # """
  # @spec test_reliable_kv_server() :: bool()
  # def test_reliable_kv_server do
  #   Emulation.init()
  #   Emulation.append_fuzzers([Fuzzers.drop(0.01), Fuzzers.delay(10.0)])
  #   spawn(:server, &reliable_kv_server/0)
  #   pid = self()
  #   spawn(:client, fn -> test_kv_client(:server, pid) end)

  #   receive do
  #     true -> true
  #     _ -> false
  #   end
  # after
  #   Emulation.terminate()
  # end
# end


end


defmodule Dynamo.Client do
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

end