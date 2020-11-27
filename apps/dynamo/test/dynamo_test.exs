defmodule DynamoTest do
  use ExUnit.Case
  doctest Dynamo
  import Emulation, only: [spawn: 2, send: 2] 

  
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Nothing crashes during startup" do 
    Emulation.init() 
    base_config =  Dynamo.new_configuration(10, 3, 2, 2)
    # IO.puts("#{inspect(base_config)}")
    spawn(:b, fn -> Dynamo.become_server(base_config) end)
    Process.sleep(5000)
  after
    Emulation.terminate()
  end 
end
