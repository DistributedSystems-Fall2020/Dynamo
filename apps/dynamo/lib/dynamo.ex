defmodule Dynamo do
  @moduledoc """
  An implementation of the Raft consensus protocol.
  """
  # Shouldn't need to spawn anything from this module, but if you do
  # you should add spawn to the imports.
  import Emulation,
    only: [send: 2, timer: 1, now: 0, whoami: 0, cancel_timer: 1, spawn: 2]

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
    # Number of repeats for nodes in consistent hash table
    num_virtual_nodes: nil,
    # Number of nodes to replicate at
    num_replicas: nil,
    # Number of nodes that need to reply to a write operation
    num_writes: nil,
    # Number of nodes that need to reply to a read operation
    num_reads: nil,
    local_store: nil,
    ring: nil,
    pending_put_req: nil,
    pending_put_rsp: nil,
    pending_put_msg: nil,
    pending_get_req: nil,
    pending_get_rsp: nil,
    pending_get_msg: nil,
    pending_handoffs: nil,
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

  defp get_preference_helper(
         curr_count,
         curr_index,
         preference_list,
         node_set,
         count,
         initial_index,
         node_list,
         avoid,
         avoided,
         first
       ) do
    if curr_count == count or (curr_index == initial_index and not first) do
      {preference_list, avoided}
    else
      if(
        not MapSet.member?(node_set, Enum.at(node_list, curr_index)) and
          not MapSet.member?(avoid, Enum.at(node_list, curr_index))
      ) do
        preference_list = preference_list ++ [Enum.at(node_list, curr_index)]
        curr_count = curr_count + 1
        node_set = MapSet.put(node_set, Enum.at(node_list, curr_index))

        curr_index =
          if curr_index == length(node_list) - 1 do
            0
          else
            curr_index + 1
          end

        get_preference_helper(
          curr_count,
          curr_index,
          preference_list,
          node_set,
          count,
          initial_index,
          node_list,
          avoid,
          avoided,
          false
        )
      else
        avoided =
          if MapSet.member?(avoid, Enum.at(node_list, curr_index)) do
            MapSet.put(avoided, Enum.at(node_list, curr_index))
          else
            avoided
          end

        # # IO.puts("AVOIDED #{inspect(Enum.at(node_list, curr_index))} #{inspect(avoid)} #{inspect(MapSet.member?(avoid, :b))}")

        curr_index =
          if curr_index == length(node_list) - 1 do
            0
          else
            curr_index + 1
          end

        get_preference_helper(
          curr_count,
          curr_index,
          preference_list,
          node_set,
          count,
          initial_index,
          node_list,
          avoid,
          avoided,
          false
        )
      end
    end
  end

  @spec get_preference_list(any(), string(), non_neg_integer(), any()) :: list()
  defp get_preference_list(ring, key, count, failed_nodes) do
    nodeList =
      PhStTransform.transform(ring.items, %{
        Tuple => fn tuple -> Tuple.to_list(tuple) end
      })

    hash_list = Enum.map(nodeList, fn [hash | _] -> hash end)
    node_list = Enum.map(nodeList, fn [_ | node] -> node end)
    node_list = List.flatten(node_list)
    # # IO.puts("nodelist #{inspect(node_list)}")
    initial_node = Bisect.bisect_left(hash_list, Utils.hash(key))

    get_preference_helper(
      0,
      initial_node,
      [],
      MapSet.new(),
      count,
      initial_node,
      node_list,
      failed_nodes,
      MapSet.new(),
      true
    )
  end

  @spec add_nodes_to_ring(map(), non_neg_integer()) :: map()
  defp add_nodes_to_ring(state, pos) do
    if pos == length(state.node_list) do
      state
    else
      node = Enum.at(state.node_list, pos)
      ring = state.ring
      {:ok, ring} = HashRing.add_node(state.ring, node)
      add_nodes_to_ring(%{state | ring: ring}, pos + 1)
    end
  end

  @spec become_server(%Dynamo{}) :: no_return()
  def become_server(state) do
    {:ok, ring} = HashRing.add_node(state.ring, whoami())

    state = %{
      state
      | local_store: %{},
        pending_put_req: %{},
        pending_put_rsp: %{},
        pending_put_msg: %{},
        pending_get_req: %{},
        pending_get_rsp: %{},
        pending_get_msg: %{},
        failed_nodes: MapSet.new(),
        pending_handoffs: %{}
    }

    state = add_nodes_to_ring(state, 0)
    timer = Emulation.timer(500, :retry)
    timer = Emulation.timer(200, :antientropy)
    server(state, false)
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
            # IO.puts("Calling GET Send #{inspect(me)} #{inspect(seq_no)}")
            {:ok, pending_get_req_seq_no} =
              Map.fetch(state.pending_get_req, seq_no)

            pending_get_req_seq_no = MapSet.put(pending_get_req_seq_no, head)

            pending_get_req =
              Map.put(state.pending_get_req, seq_no, pending_get_req_seq_no)

            timer = Emulation.timer(100, {:timer, me, head, seq_no})

            # IO.puts("Inserting timer for GET #{inspect(me)} #{inspect(head)} #{inspect(seq_no)} #{inspect(timer)}")
            msg_timers =
              Map.put(state.msg_timers, {:timer, me, head, seq_no}, timer)

            state = %{
              state
              | msg_timers: msg_timers,
                pending_get_req: pending_get_req
            }

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
            # IO.puts("Calling PUT Send #{inspect(me)} #{inspect(seq_no)}")
            {:ok, pending_put_req_seq_no} =
              Map.fetch(state.pending_put_req, seq_no)

            pending_put_req_seq_no = MapSet.put(pending_put_req_seq_no, head)

            pending_put_req =
              Map.put(state.pending_put_req, seq_no, pending_put_req_seq_no)

            timer = Emulation.timer(100, {:timer, me, head, seq_no})

            # IO.puts("Inserting timer for PUT #{inspect(me)} #{inspect(head)} #{inspect(seq_no)} #{inspect(timer)}")
            msg_timers =
              Map.put(state.msg_timers, {:timer, me, head, seq_no}, timer)

            state = %{
              state
              | msg_timers: msg_timers,
                pending_put_req: pending_put_req
            }

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

          # IO.puts("Calling nobody")
          true ->
            nil
        end
      else
        send_to_all_members(tail, state, seq_no, msg)
      end
    else
      state
    end
  end

  @spec send_to_first_members(list(), map(), map(), non_neg_integer(), any()) ::
          map()
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
          timer = Emulation.timer(100, {:timer, me, head, seq_no})

          # IO.puts("Inserting timer for #{inspect(me)} #{inspect(head)} #{inspect(seq_no)} #{inspect(timer)}")
          msg_timers =
            Map.put(state.msg_timers, {:timer, me, head, seq_no}, timer)

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
    seq_no = state.seq_no + 1
  end

  defp check_get_satisfied(server_resp, count, client_resp) do
    cond do
      count == 0 ->
        {true, client_resp}

      length(server_resp) < count ->
        {false, []}

      true ->
        [head | tail] = server_resp
        {server, value, metadata} = head

        if MapSet.member?(MapSet.new(client_resp), {value, metadata}) do
          check_get_satisfied(tail, count - 1, client_resp)
        else
          client_resp = client_resp ++ [{value, metadata}]
          check_get_satisfied(tail, count - 1, client_resp)
        end
    end
  end

  defp update_msg_helper(curr, handoff, msg, pending_handoffs) do
    if curr == length(handoff) do
      pending_handoffs
    else
      if Map.has_key?(pending_handoffs, Enum.at(handoff, curr)) do
        val = Map.get(pending_handoffs, Enum.at(handoff, curr))
        val = val ++ [msg]

        pending_handoffs =
          Map.put(pending_handoffs, Enum.at(handoff, curr), val)

        update_msg_helper(curr + 1, handoff, msg, pending_handoffs)
      else
        pending_handoffs =
          Map.put(pending_handoffs, Enum.at(handoff, curr), [msg])

        update_msg_helper(curr + 1, handoff, msg, pending_handoffs)
      end
    end
  end

  defp update_msg(handoff, msg, pending_handoffs) do
    if pending_handoffs == nil do
      %{}
    else
      handoff = MapSet.to_list(handoff)
      update_msg_helper(0, handoff, msg, pending_handoffs)
    end
  end

  defp get_not_me(preference_list) do
    if preference_list != [] do
      [head | tail] = preference_list

      if head == whoami() do
        get_not_me(tail)
      else
        head
      end
    else
      whoami()
    end
  end

  defp combine_local_stores(state, local_store, sender_keys) do
    if sender_keys == [] do
      state
    else
      [head | tail] = sender_keys

      if Map.has_key?(state.local_store, head) do
        {:ok, {target_value, target_clock}} = Map.fetch(state.local_store, head)
        {:ok, {sender_value, sender_clock}} = Map.fetch(local_store, head)
        is_lesser = Dynamo.VectorClock.less_than(target_clock, sender_clock)

        if is_lesser do
          updated_local_store =
            Map.put(state.local_store, head, {sender_value, sender_clock})

          state = %{state | local_store: updated_local_store}
          combine_local_stores(state, local_store, tail)
        else
          combine_local_stores(state, local_store, tail)
        end
      else
        combine_local_stores(state, local_store, tail)
      end
    end
  end

  def server(state, extra_state) do
    me = whoami()

    receive do
      {sender,
       %Dynamo.Client.GetMessage{
         key: key,
         metadata: metadata,
         client: client
       }} ->
        {preference_list, avoided} =
          get_preference_list(
            state.ring,
            key,
            state.num_replicas,
            state.failed_nodes
          )

        if not Enum.member?(preference_list, whoami()) and
             length(preference_list) >= 1 do
          send(Enum.at(preference_list, 0), %Dynamo.Client.GetMessage{
            key: key,
            metadata: metadata,
            client: client
          })

          server(state, extra_state)
        end

        if extra_state do
          who_to_send = get_not_me(preference_list)

          send(who_to_send, %Dynamo.Client.GetMessage{
            key: key,
            metadata: metadata,
            client: client
          })

          server(state, extra_state)
        end

        delay = Enum.random(Enum.to_list(100..200))
        # IO.puts("Delay: #{inspect(delay)}")
        Process.sleep(delay)

        seq_no = get_next_seq_no(state)
        # IO.puts("Server received GET: #{inspect(me)} #{seq_no}")
        pending_get_req = Map.put(state.pending_get_req, seq_no, MapSet.new())
        state = %{state | seq_no: seq_no, pending_get_req: pending_get_req}

        state =
          send_to_all_members(
            preference_list,
            state,
            seq_no,
            %Dynamo.GetRequest{
              key: key,
              metadata: metadata,
              seq_no: seq_no,
              handoff: nil
            }
          )

        me = whoami()
        {:ok, pending_get_req_seq_no} = Map.fetch(state.pending_get_req, seq_no)
        pending_get_req_seq_no = MapSet.put(pending_get_req_seq_no, me)

        pending_get_req =
          Map.put(state.pending_get_req, seq_no, pending_get_req_seq_no)

        state = %{state | pending_get_req: pending_get_req}

        # IO.puts("Who got the GET request? #{inspect(me)} #{inspect(state.pending_get_req)}")

        # # IO.puts("The preference list is #{inspect(preference_list)}")
        # IO.puts("get message works: #{inspect(whoami())}")  

        pending_get_msg =
          Map.put(state.pending_get_msg, seq_no, %Dynamo.Client.GetMessage{
            key: key,
            metadata: metadata,
            client: client
          })

        # # IO.puts("Pending get: #{inspect(pending_get_rsp)}")
        state = %{state | pending_get_msg: pending_get_msg}
        {value, metadata} = retrieve(state, key)

        if {value, metadata} == {nil, nil} do
          pending_get_rsp = Map.put(state.pending_get_rsp, seq_no, MapSet.new())
          state = %{state | pending_get_rsp: pending_get_rsp}
          server(state, extra_state)
        end

        pending_get_rsp =
          Map.put(
            state.pending_get_rsp,
            seq_no,
            MapSet.new([{me, value, metadata}])
          )

        state = %{state | pending_get_rsp: pending_get_rsp}

        {is_enough, responses} =
          check_get_satisfied(
            MapSet.to_list(state.pending_get_rsp[seq_no]),
            state.num_writes,
            []
          )

        if is_enough do
          # coalesce of clocks
          result = Dynamo.VectorClock.coalesce2(responses)
          # IO.puts("GET RESP - #{inspect(responses)}")
          {:ok, msg} = Map.fetch(state.pending_get_msg, seq_no)
          send(msg.client, {key, result})
          pending_get_req = Map.delete(state.pending_get_req, seq_no)
          pending_get_rsp = Map.delete(state.pending_get_rsp, seq_no)
          pending_get_msg = Map.delete(state.pending_get_msg, seq_no)

          state = %{
            state
            | pending_get_req: pending_get_req,
              pending_get_rsp: pending_get_rsp,
              pending_get_msg: pending_get_msg
          }

          server(state, extra_state)
        end

        # # IO.puts("Retrieved the message #{inspect(msg)}")     
        server(state, extra_state)

      {sender,
       %Dynamo.Client.PutMessage{
         key: key,
         value: value,
         metadata: metadata,
         client: client
       }} ->
        # Changed write replicas
        {preference_list, avoided} =
          get_preference_list(
            state.ring,
            key,
            state.num_replicas,
            state.failed_nodes
          )

        if not Enum.member?(preference_list, whoami()) and
             length(preference_list) >= 1 do
          send(Enum.at(preference_list, 0), %Dynamo.Client.PutMessage{
            key: key,
            value: value,
            metadata: metadata,
            client: client
          })

          server(state, extra_state)
        end

        if extra_state do
          who_to_send = get_not_me(preference_list)

          send(who_to_send, %Dynamo.Client.PutMessage{
            key: key,
            value: value,
            metadata: metadata,
            client: client
          })

          server(state, extra_state)
        end

        # IO.puts("Server received PUT")

        delay = Enum.random(Enum.to_list(50..200))
        # IO.puts("Delay: #{inspect(delay)}")
        Process.sleep(delay)

        seq_no = get_next_seq_no(state)
        # IO.puts("Server received PUT: #{inspect(me)} #{seq_no}")
        metadata =
          Dynamo.VectorClock.update_vector_clock(metadata, whoami(), seq_no)

        pending_put_req = Map.put(state.pending_put_req, seq_no, MapSet.new())
        state = %{state | seq_no: seq_no, pending_put_req: pending_put_req}

        state =
          send_to_all_members(
            preference_list,
            state,
            seq_no,
            %Dynamo.PutRequest{
              key: key,
              value: value,
              metadata: metadata,
              seq_no: seq_no,
              handoff: nil
            }
          )

        me = whoami()
        {:ok, pending_put_req_seq_no} = Map.fetch(state.pending_put_req, seq_no)
        pending_put_req_seq_no = MapSet.put(pending_put_req_seq_no, me)

        pending_put_req =
          Map.put(state.pending_put_req, seq_no, pending_put_req_seq_no)

        state = %{state | pending_put_req: pending_put_req}

        # IO.puts("Who got the PUT request? #{inspect(state.pending_put_req)}")

        # IO.puts("put message works #{inspect(whoami())}")
        state = store(state, key, value, metadata)

        pending_put_rsp =
          Map.put(state.pending_put_rsp, seq_no, MapSet.new([me]))

        pending_put_msg =
          Map.put(state.pending_put_msg, seq_no, %Dynamo.Client.PutMessage{
            key: key,
            value: value,
            metadata: metadata,
            client: client
          })

        state = %{
          state
          | pending_put_rsp: pending_put_rsp,
            pending_put_msg: pending_put_msg
        }

        if MapSet.size(state.pending_put_rsp[seq_no]) >= state.num_reads do
          # IO.puts("PUT RESP")
          {:ok, msg} = Map.fetch(state.pending_put_msg, seq_no)
          send(msg.client, {:ok, key})
          pending_put_req = Map.delete(state.pending_put_req, seq_no)
          pending_put_rsp = Map.delete(state.pending_put_rsp, seq_no)
          pending_put_msg = Map.delete(state.pending_put_msg, seq_no)

          state = %{
            state
            | pending_put_req: pending_put_req,
              pending_put_rsp: pending_put_rsp,
              pending_put_msg: pending_put_msg
          }

          server(state, extra_state)
        end

        server(state, extra_state)

      {sender,
       %Dynamo.GetRequest{
         key: key,
         metadata: metadata,
         seq_no: seq_no,
         handoff: handoff
       }} ->
        if extra_state do
          server(state, extra_state)
        end

        me = whoami()
        # if me == :c do
        #   server(state, nil)
        # end
        # IO.puts("Server received get request")
        {value, metadata} = retrieve(state, key)

        if {value, metadata} == {nil, nil} do
          server(state, extra_state)
        end

        send(sender, %Dynamo.GetResponse{
          key: key,
          value: value,
          metadata: metadata,
          seq_no: seq_no
        })

        server(state, extra_state)

      {sender,
       %Dynamo.PutRequest{
         key: key,
         value: value,
         metadata: metadata,
         seq_no: seq_no,
         handoff: handoff
       }} ->
        me = whoami()
        # if me == :c do
        #   server(state, nil)
        # end
        # IO.puts("Server received put request: #{inspect(me)}")
        state = store(state, key, value, metadata)

        msg = %Dynamo.PutRequest{
          key: key,
          value: value,
          metadata: metadata,
          seq_no: seq_no,
          handoff: nil
        }

        if extra_state do
          server(state, extra_state)
        end

        handoff = MapSet.new([handoff])
        pending_handoffs = state.pending_handoffs

        pending_handoffs =
          if handoff != nil do
            update_msg(handoff, msg, pending_handoffs)
          else
            pending_handoffs
          end

        # if handoff != nil do
        #   handoff = MapSet.to_list(handoff)
        #   #update pending handoffs 
        #   # Enum
        #   # IO.puts("I got a handoff: #{inspect(me)} #{inspect(handoff)}")
        # end
        send(sender, %Dynamo.PutResponse{
          key: key,
          value: value,
          metadata: metadata,
          status: :ok,
          seq_no: seq_no
        })

        server(%{state | pending_handoffs: pending_handoffs}, extra_state)

      {sender,
       %Dynamo.GetResponse{
         key: key,
         value: value,
         metadata: metadata,
         seq_no: seq_no
       }} ->
        # IO.puts("Server received get response")
        if Map.has_key?(state.msg_timers, {:timer, me, sender, seq_no}) do
          {:ok, timer} =
            Map.fetch(state.msg_timers, {:timer, me, sender, seq_no})

          k = Emulation.cancel_timer(timer)

          msg_timers =
            Map.delete(state.msg_timers, {:timer, me, sender, seq_no})

          state = %{state | msg_timers: msg_timers}

          if value == nil do
            server(state, extra_state)
          end

          if Map.has_key?(state.pending_get_rsp, seq_no) do
            {:ok, mpset_seq_no} = Map.fetch(state.pending_get_rsp, seq_no)
            mpset_seq_no = MapSet.put(mpset_seq_no, {sender, value, metadata})

            state = %{
              state
              | pending_get_rsp:
                  Map.put(state.pending_get_rsp, seq_no, mpset_seq_no)
            }

            {is_enough, responses} =
              check_get_satisfied(
                MapSet.to_list(state.pending_get_rsp[seq_no]),
                state.num_writes,
                []
              )

            # IO.puts("What did we get: #{inspect(is_enough)} #{inspect(responses)}")
            if is_enough do
              # coalesce of clocks
              result = Dynamo.VectorClock.coalesce2(responses)
              # IO.puts("GET RESP - #{inspect(responses)}")
              {:ok, msg} = Map.fetch(state.pending_get_msg, seq_no)
              send(msg.client, {key, result})
              pending_get_req = Map.delete(state.pending_get_req, seq_no)
              pending_get_rsp = Map.delete(state.pending_get_rsp, seq_no)
              pending_get_msg = Map.delete(state.pending_get_msg, seq_no)

              state = %{
                state
                | pending_get_req: pending_get_req,
                  pending_get_rsp: pending_get_rsp,
                  pending_get_msg: pending_get_msg
              }

              server(state, extra_state)
            end

            server(state, extra_state)
          end

          server(state, extra_state)
        end

        server(state, extra_state)

      {sender,
       %Dynamo.PutResponse{
         key: key,
         value: value,
         metadata: metadata,
         status: status,
         seq_no: seq_no
       }} ->
        # IO.puts("Server received put response #{inspect(whoami())} #{inspect(sender)}")
        if Map.has_key?(state.msg_timers, {:timer, me, sender, seq_no}) do
          {:ok, timer} =
            Map.fetch(state.msg_timers, {:timer, me, sender, seq_no})

          k = Emulation.cancel_timer(timer)

          # IO.puts("PUT timer cancelled: #{inspect(me)} #{inspect(sender)} #{inspect(k)}")
          msg_timers =
            Map.delete(state.msg_timers, {:timer, me, sender, seq_no})

          state = %{state | msg_timers: msg_timers}

          if status == :ok do
            if Map.has_key?(state.pending_put_rsp, seq_no) do
              {:ok, mpset_seq_no} = Map.fetch(state.pending_put_rsp, seq_no)
              mpset_seq_no = MapSet.put(mpset_seq_no, sender)

              state = %{
                state
                | pending_put_rsp:
                    Map.put(state.pending_put_rsp, seq_no, mpset_seq_no)
              }

              if MapSet.size(state.pending_put_rsp[seq_no]) >= state.num_reads do
                # IO.puts("PUT RESP")
                {:ok, msg} = Map.fetch(state.pending_put_msg, seq_no)
                send(msg.client, {:ok, key})
                pending_put_req = Map.delete(state.pending_put_req, seq_no)
                pending_put_rsp = Map.delete(state.pending_put_rsp, seq_no)
                pending_put_msg = Map.delete(state.pending_put_msg, seq_no)

                state = %{
                  state
                  | pending_put_req: pending_put_req,
                    pending_put_rsp: pending_put_rsp,
                    pending_put_msg: pending_put_msg
                }

                server(state, extra_state)
              end

              server(state, extra_state)
            end

            server(state, extra_state)
          end

          server(state, extra_state)
        end

        server(state, extra_state)

      {:timer, me, sender, seq_no} ->
        # IO.puts("timer went off: #{inspect(me)} #{inspect(sender)} #{inspect(seq_no)}")
        status = Map.fetch(state.msg_timers, {:timer, me, sender, seq_no})

        if status == :error do
          server(state, extra_state)
        end

        {:ok, timer} = status
        Emulation.cancel_timer(timer)
        msg_timers = Map.delete(state.msg_timers, {:timer, me, sender, seq_no})
        state = %{state | msg_timers: msg_timers}

        cond do
          Map.has_key?(state.pending_put_msg, seq_no) ->
            # IO.puts("Timer for PUT seq_no: #{inspect(seq_no)}")
            failed_nodes = MapSet.put(state.failed_nodes, sender)
            state = %{state | failed_nodes: failed_nodes}
            {:ok, put_msg} = Map.fetch(state.pending_put_msg, seq_no)
            # Changed write replicas
            {preference_list, avoided} =
              get_preference_list(
                state.ring,
                put_msg.key,
                state.num_replicas,
                state.failed_nodes
              )

            # IO.inspect(preference_list)
            send_msg = %Dynamo.PutRequest{
              key: put_msg.key,
              value: put_msg.value,
              metadata: put_msg.metadata,
              seq_no: seq_no,
              handoff: sender
            }

            {status, state, pending_put_req} =
              send_to_first_members(
                preference_list,
                state,
                state.pending_put_req,
                seq_no,
                send_msg
              )

            state = %{state | pending_put_req: pending_put_req}

            # IO.puts("Preference list after PUT timer: #{inspect(preference_list)} #{inspect(state.pending_put_req)}")
            server(state, extra_state)

          Map.has_key?(state.pending_get_msg, seq_no) ->
            # IO.puts("Timer for GET seq_no: #{inspect(seq_no)}")
            # IO.puts("GET SENT before timer #{inspect(state.pending_get_req)}")
            failed_nodes = MapSet.put(state.failed_nodes, sender)
            {:ok, get_msg} = Map.fetch(state.pending_get_msg, seq_no)

            {preference_list, avoided} =
              get_preference_list(
                state.ring,
                get_msg.key,
                state.num_replicas,
                state.failed_nodes
              )

            send_msg = %Dynamo.GetRequest{
              key: get_msg.key,
              metadata: get_msg.metadata,
              seq_no: seq_no,
              handoff: sender
            }

            {status, state, pending_get_req} =
              send_to_first_members(
                preference_list,
                state,
                state.pending_get_req,
                seq_no,
                send_msg
              )

            state = %{state | pending_get_req: pending_get_req}

            # IO.puts("Preference list after GET timer: #{inspect(preference_list)} #{inspect(state.pending_get_req)}")
            server(state, extra_state)

          # IO.puts("Timer for empty")
          true ->
            server(state, extra_state)
        end

        server(state, extra_state)

      {sender, :ping} ->
        # send a ping resp 
        send(sender, :pingresp)
        server(state, extra_state)

      {sender, :pingresp} ->
        # IO.puts("Hinted handoff")
        # things to fix, have a pending handoffs map
        # for handoff sent avoided instead 
        # @TODO check if the sender is in failed nodes, remove 
        # check if sending in pending handoffs, send and then remove
        failed_nodes = MapSet.delete(state.failed_nodes, sender)
        list_of_pending = Map.get(state.pending_handoffs, sender)

        if list_of_pending != nil do
          Enum.each(list_of_pending, fn msg ->
            send(sender, msg)
          end)
        end

        pending =
          if list_of_pending == nil do
            state.pending_handoffs
          else
            Map.delete(state.pending_handoffs, sender)
          end

        server(%{state | failed_nodes: failed_nodes}, extra_state)

      {sender, {:fail, time_to_fail}} ->
        # IO.puts("Beginning to sleep: #{inspect(whoami())}")
        # Process.sleep(time_to_fail)
        timer = Emulation.timer(time_to_fail, :recover)
        extra_state = true
        server(state, extra_state)

      :retry ->
        # Send a ping to all failed nodes 
        timer = Emulation.timer(500, :retry)
        failed_nodes = MapSet.to_list(state.failed_nodes)
        Enum.each(failed_nodes, fn x -> send(x, :ping) end)
        server(state, extra_state)

      {sender, :alive} ->
        send(sender, :alive)
        server(state, extra_state)

      :recover ->
        extra_state = false
        server(state, extra_state)

      :antientropy ->
        node_list = List.delete(state.node_list, me)
        match_node = Enum.random(node_list)
        send(match_node, {:antientropy_request, state.local_store})
        timer = Emulation.timer(200, :antientropy)
        server(state, extra_state)

      {sender, {:antientropy_request, local_store}} ->
        sender_keys = Map.keys(local_store)
        state = combine_local_stores(state, local_store, sender_keys)
        send(sender, {:antientropy_response, state.local_store})
        server(state, extra_state)

      {sender, {:antientropy_response, local_store}} ->
        sender_keys = Map.keys(local_store)
        state = combine_local_stores(state, local_store, sender_keys)
        server(state, extra_state)
    end
  end

  defp print_st(state) do
    # IO.puts("----- Printing state ----")
    # IO.puts("#{inspect(state)}")
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
      {nil, nil}
    end
  end

  @spec test_add_to_ring() :: no_return()
  def test_add_to_ring() do
    state = new_configuration(10, 3, 2, 2, [:b, :c])
    state = add_nodes_to_ring(state, 0)

    # IO.puts("Testing store: #{inspect(state.ring)}")
  end

  @spec test_store() :: no_return()
  def test_store() do
    key = "ag"
    value = 1
    metadata = %{}
    state = new_configuration(10, 3, 2, 2, [:b, :c])
    state = %{state | local_store: %{}}
    state = store(state, key, value, metadata)
    # IO.puts("Testing store: #{inspect(state)}")
  end

  @spec test_retrieve() :: no_return()
  def test_retrieve() do
    key = "ag"
    value = 1
    metadata = %{}
    state = new_configuration(10, 3, 2, 2, [:b, :c])
    state = %{state | local_store: %{}}
    state = store(state, key, value, metadata)
    value = retrieve(state, key)
    # IO.puts("Testing retrieve: #{inspect(value)}")
  end

  @spec test_combine_local_stores() :: no_return()
  def test_combine_local_stores() do
    recv_state = new_configuration(10, 3, 2, 2, [:s, :t])
    send_state = new_configuration(10, 3, 2, 2, [:s, :t])
    recv_state = %{recv_state | local_store: %{}}
    send_state = %{send_state | local_store: %{}}

    recv_clock =
      Dynamo.VectorClock.update_vector_clock(%Dynamo.VectorClock{}, :s, 1)

    send_clock =
      Dynamo.VectorClock.update_vector_clock(%Dynamo.VectorClock{}, :s, 2)

    recv_state = store(recv_state, "k", 1, recv_clock)
    send_state = store(send_state, "k", 2, send_clock)
    recv_state = combine_local_stores(recv_state, send_state.local_store, ["k"])
    # IO.puts("RECV: #{inspect(recv_state.local_store)}")
  end
end

defmodule Dynamo.Client do
  import Emulation, only: [send: 2, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  alias __MODULE__
  @enforce_keys [:node_list]
  defstruct(node_list: nil, test_client: nil, local_store: nil)
  @spec new_client(atom()) :: %Dynamo.Client{node_list: atom()}
  def new_client(node_list) do
    # # IO.puts("CREATING A CLIENT")
    %Dynamo.Client{node_list: node_list, test_client: nil, local_store: %{}}
  end

  @spec client(%Dynamo.Client{}) :: no_return()
  def client(state) do
    receive do
      {sender,
       %Dynamo.ToClientPutMessage{
         key: key,
         value: value,
         metadata: metadata,
         node_list: node_list
       }} ->
        # IO.puts("Client put: #{inspect(state.test_client)}")
        me = whoami()

        node_list =
          if node_list == nil do
            state.node_list
          else
            node_list
          end

        server = Enum.random(node_list)
        metadata = Map.get(state.local_store, key, [])

        metadata =
          if length(metadata) == 0 do
            %Dynamo.VectorClock{}
          else
            Dynamo.VectorClock.converge(metadata)
          end

        # IO.puts("Converged - #{inspect(metadata)}")
        send(server, %Dynamo.Client.PutMessage{
          key: key,
          metadata: metadata,
          value: value,
          client: me
        })

        # IO.puts("Client put 2")
        client(%{state | test_client: sender})

      {sender,
       %Dynamo.ToClientGetMessage{
         key: key,
         metadata: metadata,
         node_list: node_list
       }} ->
        # IO.puts("Client get: #{inspect(state.test_client)}")
        me = whoami()

        node_list =
          if node_list == nil do
            state.node_list
          else
            node_list
          end

        server = Enum.random(node_list)

        send(server, %Dynamo.Client.GetMessage{
          key: key,
          metadata: metadata,
          client: me
        })

        client(%{state | test_client: sender})

      {sender, {:ok, key}} ->
        # IO.puts("Got response")
        # IO.puts("Test Client: #{inspect(state.test_client)}")
        send(state.test_client, {:put, :ok, key})
        client(state)

      {sender, {key, responses}} ->
        # IO.puts("CLOCK_LIST GEN - #{inspect(state.test_client)}")
        send(state.test_client, {:get, key, responses})
        local_store = state.local_store
        clock_list = Enum.map(responses, fn {a, b} -> b end)
        # IO.puts("CLOCK_LIST GEN - #{inspect(key)}")
        local_store = Map.put(local_store, key, clock_list)
        client(%{state | local_store: local_store})

      {sender, :change_test} ->
        state = %{state | test_client: sender}
        client(state)
    end
  end
end
