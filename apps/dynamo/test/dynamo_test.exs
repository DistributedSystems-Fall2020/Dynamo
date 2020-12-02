#The test cases are in charge of generating a random server for each of the client requests


defmodule DynamoTest do
  use ExUnit.Case
  doctest Dynamo
  import Emulation, only: [spawn: 2, send: 2] 

  
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Nothing crashes during startup" do 
    Emulation.init() 
    base_config =  Dynamo.new_configuration(10, 3, 2, 2, [:b, :c])
    # IO.puts("#{inspect(base_config)}")
    spawn(:b, fn -> Dynamo.become_server(base_config) end)
    spawn(:c, fn -> Dynamo.become_server(base_config) end)
    client = 
      spawn(:client, fn -> 
        IO.puts("I'm here")
        client = Dynamo.Client.new_client(:b) 
        IO.puts("Client: #{inspect(client)}")
        Dynamo.Client.put(client, "key", %{}, 1, :b)
      end)
    handle = Process.monitor(client)
    Process.sleep(5000)
    client2 = 
      spawn(:client2, fn -> 
        IO.puts("I'm here")
        client = Dynamo.Client.new_client(:b) 
        IO.puts("Client: #{inspect(client)}")
        Dynamo.Client.get(client, "key", %{}, :b)
      end)
    handle = Process.monitor(client2)

    Process.sleep(5000)
  after
    Emulation.terminate()
  end 
end
