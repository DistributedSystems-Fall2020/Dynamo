#The test cases are in charge of generating a random server for each of the client requests


defmodule DynamoTest do
  use ExUnit.Case
  doctest Dynamo
  import Emulation, only: [spawn: 2, send: 2] 

  
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Nothing crashes during startup" do 
    Emulation.init() 
    base_config =  Dynamo.new_configuration(10, 3, 2, 2, [:a, :b, :c, :d, :e, :f, :g, :h, :i, :j])
    base_client_config = Dynamo.Client.new_client([:b, :c, :d, :e, :f])
    # base_config =  Dynamo.new_configuration(10, 3, 2, 2, [:b, :c, :d])
    # IO.puts("#{inspect(base_config)}")
    a = spawn(:a, fn -> Dynamo.become_server(base_config) end)
    b = spawn(:b, fn -> Dynamo.become_server(base_config) end)
    c = spawn(:c, fn -> Dynamo.become_server(base_config) end)
    d = spawn(:d, fn -> Dynamo.become_server(base_config) end)
    e = spawn(:e, fn -> Dynamo.become_server(base_config) end)
    f = spawn(:f, fn -> Dynamo.become_server(base_config) end)
    g = spawn(:g, fn -> Dynamo.become_server(base_config) end)
    h = spawn(:h, fn -> Dynamo.become_server(base_config) end)
    i = spawn(:i, fn -> Dynamo.become_server(base_config) end)
    j = spawn(:j, fn -> Dynamo.become_server(base_config) end)
    client = spawn(:client, fn -> Dynamo.Client.client(base_client_config) end)

    tester = 
      spawn(:tester, fn ->
        # IO.puts("HI")
        send(:client, %Dynamo.ToClientPutMessage{
          key: "key", 
          value: 1, 
          metadata: nil, 
          node_list: nil
        })
        Process.sleep(20)
        # send(:g, %Dynamo.ToClientGetMessage{
        #   key: "key", 
        #   metadata: nil, 
        #   node_list: nil})

        # Process.sleep(20)
        # send(:g, %Dynamo.ToClientPutMessage{
        #   key: "key", 
        #   value: 4, 
        #   metadata: nil, 
        #   node_list: nil})

          receive do 
            {sender, {:put, :ok, key}} -> 
              IO.puts("Test put 1 - #{inspect(key)}")

            {sender, {:get, key, responses}} -> 
              IO.puts("Test get 1 - #{inspect(responses)}")
          end
          IO.puts("start")
          Process.sleep(1000)
          IO.puts("end")
        end
      )

    Process.sleep(100)
    tester2 = 
      spawn(:tester2, fn ->
        # IO.puts("Hello")
        # send(:g, %Dynamo.ToClientPutMessage{
        #   key: "key", 
        #   value: 1, 
        #   metadata: nil, 
        #   node_list: nil
        # })
        # Process.sleep(20)
        send(:client, %Dynamo.ToClientGetMessage{
          key: "key", 
          metadata: nil, 
          node_list: nil})

        # Process.sleep(20)
        # send(:g, %Dynamo.ToClientPutMessage{
        #   key: "key", 
        #   value: 4, 
        #   metadata: nil, 
        #   node_list: nil})

          receive do 
            {sender, {:put, :ok, key}} -> 
              IO.puts("Test put 2 - #{inspect(key)}")

            {sender, {:get, key, responses}} -> 
              IO.puts("Test get 2 - #{inspect(responses)}")
          end
          Process.sleep(1000)
        end
      )

      Process.sleep(100)
      tester3 = 
      spawn(:tester3, fn ->
        IO.puts("Hello")
        send(:client, %Dynamo.ToClientPutMessage{
          key: "key", 
          value: 2, 
          metadata: nil, 
          node_list: nil
        })
        # Process.sleep(20)
        # send(:g, %Dynamo.ToClientGetMessage{
        #   key: "key", 
        #   metadata: nil, 
        #   node_list: nil})

        # Process.sleep(20)
        # send(:g, %Dynamo.ToClientPutMessage{
        #   key: "key", 
        #   value: 4, 
        #   metadata: nil, 
        #   node_list: nil})

          receive do 
            {sender, {:put, :ok, key}} -> 
              IO.puts("Test put 3 - #{inspect(key)}")

            {sender, {:get, key, responses}} -> 
              IO.puts("Test get 3 - #{inspect(responses)}")
          end
          Process.sleep(1000)
        end
      )

      Process.sleep(100)
      # tester4 = 
      # spawn(:tester4, fn ->
      #   # IO.puts("Hello")
      #   # send(:g, %Dynamo.ToClientPutMessage{
      #   #   key: "key", 
      #   #   value: 1, 
      #   #   metadata: nil, 
      #   #   node_list: nil
      #   # })
      #   # Process.sleep(20)
      #   send(:client, %Dynamo.ToClientGetMessage{
      #     key: "key", 
      #     metadata: nil, 
      #     node_list: nil})

      #   # Process.sleep(20)
      #   # send(:g, %Dynamo.ToClientPutMessage{
      #   #   key: "key", 
      #   #   value: 4, 
      #   #   metadata: nil, 
      #   #   node_list: nil})

      #     receive do 
      #       {sender, {:put, :ok, key}} -> 
      #         IO.puts("Test put 4 - #{inspect(key)}")

      #       {sender, {:get, key, responses}} -> 
      #         IO.puts("Test get 4 - #{inspect(responses)}")
      #     end
      #     Process.sleep(1000)
      #   end
      # )

      # Process.sleep(100)

      tester5 = 
      spawn(:tester5, fn ->
        # IO.puts("Hello")
        send(:client, %Dynamo.ToClientPutMessage{
          key: "key", 
          value: 4, 
          metadata: nil, 
          node_list: nil
        })
        # Process.sleep(20)
        # send(:client, %Dynamo.ToClientGetMessage{
        #   key: "key", 
        #   metadata: nil, 
        #   node_list: nil})

        # Process.sleep(20)
        # send(:g, %Dynamo.ToClientPutMessage{
        #   key: "key", 
        #   value: 4, 
        #   metadata: nil, 
        #   node_list: nil})

          receive do 
            {sender, {:put, :ok, key}} -> 
              IO.puts("Test put 5 - #{inspect(key)}")

            {sender, {:get, key, responses}} -> 
              IO.puts("Test get 5 - #{inspect(responses)}")
          end
          Process.sleep(1000)

        end
      )


      # Process.sleep(10)
      tester4 = 
      spawn(:tester4, fn ->
        IO.puts("Hello")
        # send(:client, %Dynamo.ToClientPutMessage{
        #   key: "key", 
        #   value: 1, 
        #   metadata: nil, 
        #   node_list: nil
        # })
        Process.sleep(20)
        send(:client, %Dynamo.ToClientGetMessage{
          key: "key", 
          metadata: nil, 
          node_list: nil})

        # Process.sleep(20)
        # send(:g, %Dynamo.ToClientPutMessage{
        #   key: "key", 
        #   value: 4, 
        #   metadata: nil, 
        #   node_list: nil})

          receive do 
            {sender, {:put, :ok, key}} -> 
              IO.puts("Test put 4 - #{inspect(key)}")

            {sender, {:get, key, responses}} -> 
              IO.puts("Test get 4 - #{inspect(responses)}")
          end
          Process.sleep(1000)
        end
      )
    # client = 
    #   spawn(:client, fn -> 
    #     client = Dynamo.Client.new_client([:b, :c, :d, :e, :f]) 
    #     IO.puts("Client: #{inspect(client)}")
    #     Dynamo.Client.put(client, "key", %{}, 1, :b)
    #     receive do
    #       {sender, {:ok, key}} -> IO.puts("HEYEE")
    #     end
    #   end)
    # handle = Process.monitor(client)
    # Process.sleep(5000)
    # client3 = 
    #   spawn(:client3, fn -> 
    #     client = Dynamo.Client.new_client([:b, :c, :d, :e, :f]) 
    #     IO.puts("Client: #{inspect(client)}")
    #     Dynamo.Client.get(client, "key", %{}, :b)
    #     receive do
    #       {sender, {key, responses}} -> IO.puts("We is good: #{inspect(responses)}")
    #     end
    #   end)
    # handle = Process.moßnitor(client3)
    Process.sleep(1000)
  after
    Emulation.terminate()
  end 
end
