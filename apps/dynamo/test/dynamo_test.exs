#The test cases are in charge of generating a random server for each of the client requests


defmodule DynamoTest do
  use ExUnit.Case
  doctest Dynamo
  import Emulation, only: [spawn: 2, send: 2] 

  
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Nothing crashes during startup" do 
    Emulation.init() 
    base_config =  Dynamo.new_configuration(10, 1, 6, 6, [:b, :c, :d, :e, :f])
    # base_config =  Dynamo.new_configuration(10, 3, 2, 2, [:b, :c, :d])
    # IO.puts("#{inspect(base_config)}")
    b = spawn(:b, fn -> Dynamo.become_server(base_config) end)
    c = spawn(:c, fn -> Dynamo.become_server(base_config) end)
    d = spawn(:d, fn -> Dynamo.become_server(base_config) end)
    e = spawn(:e, fn -> Dynamo.become_server(base_config) end)
    f = spawn(:f, fn -> Dynamo.become_server(base_config) end)
    client = 
      spawn(:client, fn -> 
        client = Dynamo.Client.new_client([:b, :c, :d, :e, :f]) 
        IO.puts("Client: #{inspect(client)}")
        Dynamo.Client.put(client, "key", %{}, 1, :b)
        receive do
          {sender, {:ok, key}} -> IO.puts("HEYEE")
        end
      end)
    handle = Process.monitor(client)
    Process.sleep(5000)
    client3 = 
      spawn(:client3, fn -> 
        client = Dynamo.Client.new_client([:b, :c, :d, :e, :f]) 
        IO.puts("Client: #{inspect(client)}")
        Dynamo.Client.get(client, "key", %{}, :b)
        receive do
          {sender, {key, responses}} -> IO.puts("We is good: #{inspect(responses)}")
        end
      end)
    handle = Process.monitor(client3)
    Process.sleep(5000)
  after
    Emulation.terminate()
  end 
end
