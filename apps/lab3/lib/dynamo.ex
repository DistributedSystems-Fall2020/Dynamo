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

  @spec reliable_kv_server(map(), number()) :: no_return()
  defp reliable_kv_server(state) do
    receive do
      {sender, {@get, key}} ->
        # TODO: Send a message `{key, current value(key)}`
        # to the sender using `reliable_send`.
        # If the key is not currently present send
        # `{key, nil}`. You might find
        # https://hexdocs.pm/elixir/Map.html
        # useful.
        # You should use count as the nonce for
        # reliable_send, and update it each time you
        # send a message.
        value = Map.get(state, key)
        message = {key, value}
        send(sender, message)
        reliable_kv_server(state)

      {_sender, {@set, key, {value, context}}} ->
        # TODO: Store  value for the given key
        # in the state. You should not send any
        # message to the sender. You might find
        # https://hexdocs.pm/elixir/Map.html
        # useful.
        state = Map.put(state, key, {value, context})
        reliable_kv_server(state)
    end
  end

  @spec test_kv_client(atom(), pid()) :: boolean()
  defp test_kv_client(server, caller) do
    reliable_send(server, {@set, :a, 1}, 1, @send_timeout)
    reliable_send(server, {@set, :b, 22}, 2, @send_timeout)
    reliable_send(server, {@get, :a}, 3, @send_timeout)

    case reliable_receive() do
      {^server, m} ->
        send(caller, m == {:a, 1})
        m == {:a, 1}

      _ ->
        send(caller, false)
        false
    end
  end

  @doc """
  Test reliable key value server.
  """
  @spec test_reliable_kv_server() :: bool()
  def test_reliable_kv_server do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.drop(0.01), Fuzzers.delay(10.0)])
    spawn(:server, &reliable_kv_server/0)
    pid = self()
    spawn(:client, fn -> test_kv_client(:server, pid) end)

    receive do
      true -> true
      _ -> false
    end
  after
    Emulation.terminate()
  end
end


end