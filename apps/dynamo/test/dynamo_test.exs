# The test cases are in charge of generating a random server for each of the client requests

defmodule DynamoTest do
  use ExUnit.Case
  doctest Dynamo
  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  # test "Server goes down" do 
  #   Emulation.init()
  # end 

  # defp test_client_helper(client, messages_to_send, replies) do
  #   # IO.puts("I came in here")
  #   if messages_to_send == [] do
  #     receive do
  #       {sender, {:put, :ok, key}} ->
  #         IO.puts("PUT RESP: #{inspect(key)}")
  #         # replies = replies ++ [response]
  #         # IO.inspect(response)
  #         test_client_helper(client, messages_to_send, replies)
  #       {sender, {:get, key, responses}} ->
  #         IO.puts("GET RESP: #{inspect(key)} - #{inspect(responses)}")
  #         # replies = replies ++ [response]
  #         # IO.inspect(response)
  #         test_client_helper(client, messages_to_send, replies)
  #     after
  #       10000 -> replies
  #     end
  #   else
  #     IO.puts("Putting messages")
  #     [head|tail] = messages_to_send
  #     IO.puts("Message: #{inspect(head)}")
  #     send(client, head)
  #     # Process.sleep(200)
  #     test_client_helper(client, tail, replies)
  #   end
  # end

  test "Nothing crashes during startup" do
    Emulation.init()
    # Emulation.append_fuzzers([Fuzzers.delay(10), Fuzzers.drop(0.1)])
    Emulation.append_fuzzers([Fuzzers.delay(50)])

    # base_config =  Dynamo.new_configuration(10, 3, 2, 2, [:a, :b, :c, :d, :e, :f, :g, :h, :i, :j])
    # base_client_config = Dynamo.Client.new_client([:a, :b, :c, :d, :e, :f, :g, :h, :i, :j])

    base_config = Dynamo.new_configuration(10, 3, 2, 2, [:a, :b, :c, :d, :e])
    base_client_config = Dynamo.Client.new_client([:a, :b, :c, :d, :e])

    # base_config =  Dynamo.new_configuration(10, 3, 2, 2, [:b, :c, :d])
    # IO.puts("#{inspect(base_config)}")
    a = spawn(:a, fn -> Dynamo.become_server(base_config) end)
    b = spawn(:b, fn -> Dynamo.become_server(base_config) end)
    c = spawn(:c, fn -> Dynamo.become_server(base_config) end)
    d = spawn(:d, fn -> Dynamo.become_server(base_config) end)
    e = spawn(:e, fn -> Dynamo.become_server(base_config) end)
    # f = spawn(:f, fn -> Dynamo.become_server(base_config) end)
    # g = spawn(:g, fn -> Dynamo.become_server(base_config) end)
    # h = spawn(:h, fn -> Dynamo.become_server(base_config) end)
    # i = spawn(:i, fn -> Dynamo.become_server(base_config) end)
    # j = spawn(:j, fn -> Dynamo.become_server(base_config) end)
    client = spawn(:client, fn -> Dynamo.Client.client(base_client_config) end)

    client2 =
      spawn(:client2, fn -> Dynamo.Client.client(base_client_config) end)

    client3 =
      spawn(:client3, fn -> Dynamo.Client.client(base_client_config) end)

    tester =
      spawn(:tester, fn ->
        # IO.puts("HI")
        send(:client, :change_test)

        send(:client, %Dynamo.ToClientPutMessage{
          key: "key",
          value: 1,
          metadata: nil,
          node_list: nil
        })

        # receive do
        #   {sender, {:put, :ok, key}} ->
        #     # IO.puts("Test put 1 - #{inspect(key)}")
        #     IO.puts("PUT 1: #{inspect(key)}")

        #   {sender, {:get, key, responses}} ->
        #     [{value, clock} | tail] = responses
        #     # IO.puts("Test get 1 - #{inspect(responses)}")
        #     IO.puts("GET 1: #{inspect(value)}")
        # end

        # Process.sleep(200)

        # send(:client, %Dynamo.ToClientGetMessage{
        #   key: "key",
        #   metadata: nil,
        #   node_list: nil
        # })

        # # Process.sleep(200)
        # # Process.sleep(200)
        # receive do
        #   {sender, {:put, :ok, key}} ->
        #     IO.puts("PUT 1: #{inspect(key)}")

        #   {sender, {:get, key, responses}} ->
        #     [{value, clock} | tail] = responses
        #     IO.puts("GET 1: #{inspect(value)}")
        #     # IO.puts("Test get 1 - #{inspect(responses)}")
        # end

        # IO.puts("start")
        Process.sleep(1000)
        # IO.puts("end")
      end)

    Process.sleep(200)

    # tester2 =
    #   spawn(:tester2, fn ->
    #     # IO.puts("HI")

    #     node_list = [:b, :c, :e]
    #     fail = Enum.random(node_list)
    #     send(fail, {:fail, 200})

    #     # Process.sleep(200)

    #     send(:client2, :change_test)

    #     send(:client2, %Dynamo.ToClientPutMessage{
    #       key: "key",
    #       value: 2,
    #       metadata: nil,
    #       node_list: nil
    #     })

    #     receive do
    #       {sender, {:put, :ok, key}} ->
    #         IO.puts("PUT 2: #{inspect(key)}")

    #       {sender, {:get, key, responses}} ->
    #         [{value, clock} | tail] = responses
    #         IO.puts("GET 2: #{inspect(value)}")
    #     end

    #     # IO.puts("HERE")
    #     Process.sleep(0)
    #     # IO.puts("HERE NEXT")

    #     send(:client2, %Dynamo.ToClientGetMessage{
    #       key: "key",
    #       metadata: nil,
    #       node_list: nil
    #     })

    #     # Process.sleep(200)
    #     receive do
    #       {sender, {:put, :ok, key}} ->
    #         IO.puts("PUT 2: #{inspect(key)}")

    #       {sender, {:get, key, responses}} ->
    #         [{value, clock} | tail] = responses
    #         IO.puts("GET 2: #{inspect(value)}")
    #     end

    #     # IO.puts("start")
    #     Process.sleep(1000)
    #     # IO.puts("end")
    #   end)


    tester2 =
      spawn(:tester2, fn ->

        node_list = [:b, :c, :e]
        fail = Enum.random(node_list)
        send(fail, {:fail, 200})

        send(:client2, :change_test)

        send(:client2, %Dynamo.ToClientPutMessage{
          key: "key",
          value: 2,
          metadata: nil,
          node_list: nil
        })

        send(:client2, %Dynamo.ToClientPutMessage{
          key: "key",
          value: 3,
          metadata: nil,
          node_list: nil
        })

        send(:client2, %Dynamo.ToClientPutMessage{
          key: "key",
          value: 4,
          metadata: nil,
          node_list: nil
        })

        send(:client2, %Dynamo.ToClientPutMessage{
          key: "key",
          value: 5,
          metadata: nil,
          node_list: nil
        })

        send(:client2, %Dynamo.ToClientPutMessage{
          key: "key",
          value: 6,
          metadata: nil,
          node_list: nil
        })

        # receive do
        #   {sender, {:put, :ok, key}} ->
        #     IO.puts("PUT 2: #{inspect(key)}")

        #   {sender, {:get, key, responses}} ->
        #     [{value, clock} | tail] = responses
        #     IO.puts("GET 2: #{inspect(value)}")
        # end

        # receive do
        #   {sender, {:put, :ok, key}} ->
        #     IO.puts("PUT 3: #{inspect(key)}")

        #   {sender, {:get, key, responses}} ->
        #     [{value, clock} | tail] = responses
        #     IO.puts("GET 3: #{inspect(value)}")
        # end

        # receive do
        #   {sender, {:put, :ok, key}} ->
        #     IO.puts("PUT 4: #{inspect(key)}")

        #   {sender, {:get, key, responses}} ->
        #     [{value, clock} | tail] = responses
        #     IO.puts("GET 4: #{inspect(value)}")
        # end

        # # IO.puts("HERE")
        Process.sleep(30)
        # # IO.puts("HERE NEXT")

        send(:client2, %Dynamo.ToClientGetMessage{
          key: "key",
          metadata: nil,
          node_list: nil
        })

        # Process.sleep(200)
        receive do
          # {sender, {:put, :ok, key}} ->
          #   IO.puts("PUT 5: #{inspect(key)}")

          {sender, {:get, key, responses}} ->
            [{value, clock} | tail] = responses
            IO.puts("GET 5: #{inspect(value)}")
        end

        # IO.puts("start")
        Process.sleep(1000)
        # IO.puts("end")
      end)



    # Process.sleep(1000)

    # tester3 = 
    #   spawn(:tester3, fn ->
    #     # IO.puts("HI")
    #     send(:client3, :change_test)
    #     send(:client3, %Dynamo.ToClientPutMessage{
    #       key: "key", 
    #       value: 3, 
    #       metadata: nil, 
    #       node_list: nil
    #     })
    #     # Process.sleep(200)

    #     receive do 
    #       {sender, {:put, :ok, key}} -> 
    #         IO.puts("Test put 3 - #{inspect(key)}")

    #       {sender, {:get, key, responses}} -> 
    #         IO.puts("Test get 3 - #{inspect(responses)}")
    #     end
    #     Process.sleep(200)

    #     send(:client3, %Dynamo.ToClientGetMessage{
    #       key: "key", 
    #       metadata: nil, 
    #       node_list: nil})

    #     receive do 
    #       {sender, {:put, :ok, key}} -> 
    #         IO.puts("Test put 3 - #{inspect(key)}")
    #       {sender, {:get, key, responses}} -> 
    #         IO.puts("Test get 3 - #{inspect(responses)}")
    #     end
    #     IO.puts("start")
    #     Process.sleep(1000)
    #     IO.puts("end")
    #   end
    # )

    # messages_to_send = [%Dynamo.ToClientPutMessage{
    #                       key: "key", 
    #                       value: 2, 
    #                       metadata: nil, 
    #                       node_list: nil},
    #                     %Dynamo.ToClientGetMessage{
    #                       key: "key", 
    #                       metadata: nil, 
    #                       node_list: nil} 
    #                     ]

    # send(:client, %Dynamo.ToClientPutMessage{
    #   key: "key", 
    #   value: 1, 
    #   metadata: nil, 
    #   node_list: nil})

    # receive do
    #   {sender, {:put, :ok, key}} ->
    #     response = "PUT RESP: #{inspect(key)}"
    #     IO.inspect(response)
    #   {sender, {:get, key, responses}} ->
    #     response = "GET RESP: #{inspect(key)} - #{inspect(responses)}"
    #     IO.inspect(response)
    # end

    # Process.sleep(1000)

    # receive do
    #   {sender, {:put, :ok, key}} ->
    #     response = "PUT RESP: #{inspect(key)}"
    #     IO.inspect(response)
    #   {sender, {:get, key, responses}} ->
    #     response = "GET RESP: #{inspect(key)} - #{inspect(responses)}"
    #     IO.inspect(response)
    # end
    # IO.puts("hi")
    # tester = 
    #   spawn(:tester, fn -> 
    #     replies = test_client_helper(:client, messages_to_send, [])
    #     # Process.sleep(2000)
    #     end
    #   )
    # Process.sleep(10000)

    # tester = 
    #   spawn(:tester, fn ->
    #     # IO.puts("HI")
    #     send(:client, %Dynamo.ToClientPutMessage{
    #       key: "key", 
    #       value: 1, 
    #       metadata: nil, 
    #       node_list: nil
    #     })
    #     Process.sleep(200)
    #     send(:client, %Dynamo.ToClientGetMessage{
    #       key: "key", 
    #       metadata: nil, 
    #       node_list: nil})
    #     # Process.sleep(200)

    #       receive do 
    #         {sender, {:put, :ok, key}} -> 
    #           IO.puts("Test put 1 - #{inspect(key)}")

    #         {sender, {:get, key, responses}} -> 
    #           IO.puts("Test get 1 - #{inspect(responses)}")
    #       end
    #       receive do 
    #         {sender, {:put, :ok, key}} -> 
    #           IO.puts("Test put 1 - #{inspect(key)}")

    #         {sender, {:get, key, responses}} -> 
    #           IO.puts("Test get 1 - #{inspect(responses)}")
    #       end
    #       IO.puts("start")
    #       Process.sleep(1000)
    #       IO.puts("end")
    #     end
    #   )

    # Process.sleep(500)

    # tester2 = 
    #   spawn(:tester2, fn -> 
    #     send(:client2, :change_test)
    #     replies = test_client_helper(:client2, messages_to_send, [])
    #     end
    #   )

    # Process.sleep(100)
    # tester2 = 
    #   spawn(:tester2, fn ->
    #     # IO.puts("Hello")
    #     send(:client2, %Dynamo.ToClientPutMessage{
    #       key: "key", 
    #       value: 2, 
    #       metadata: nil, 
    #       node_list: nil
    #     })

    #     receive do 
    #       {sender, {:put, :ok, key}} -> 
    #         IO.puts("Test put 2 - #{inspect(key)}")

    #       {sender, {:get, key, responses}} -> 
    #         IO.puts("Test get 2 - #{inspect(responses)}")
    #     end
    #     Process.sleep(20)

    #     send(:client2, %Dynamo.ToClientGetMessage{
    #       key: "key", 
    #       metadata: nil, 
    #       node_list: nil})

    # send(:client2, %Dynamo.ToClientPutMessage{
    #   key: "key", 
    #   value: 3, 
    #   metadata: nil, 
    #   node_list: nil
    # })
    # # Process.sleep(20)
    # send(:client2, %Dynamo.ToClientGetMessage{
    #   key: "key", 
    #   metadata: nil, 
    #   node_list: nil})

    # send(:client2, %Dynamo.ToClientPutMessage{
    #   key: "key", 
    #   value: 4, 
    #   metadata: nil, 
    #   node_list: nil
    # })
    # # Process.sleep(20)
    # send(:client2, %Dynamo.ToClientGetMessage{
    #   key: "key", 
    #   metadata: nil, 
    #   node_list: nil})

    #     # Process.sleep(20)
    #     # send(:g, %Dynamo.ToClientPutMessage{
    #     #   key: "key", 
    #     #   value: 4, 
    #     #   metadata: nil, 
    #     #   node_list: nil})

    # receive do 
    #   {sender, {:put, :ok, key}} -> 
    #     IO.puts("Test put 3 - #{inspect(key)}")

    #   {sender, {:get, key, responses}} -> 
    #     IO.puts("Test get 3 - #{inspect(responses)}")
    # end
    # receive do 
    #   {sender, {:put, :ok, key}} -> 
    #     IO.puts("Test put 4 - #{inspect(key)}")

    #   {sender, {:get, key, responses}} -> 
    #     IO.puts("Test get 4 - #{inspect(responses)}")
    # end
    # receive do 
    #   {sender, {:put, :ok, key}} -> 
    #     IO.puts("Test put 5 - #{inspect(key)}")

    #   {sender, {:get, key, responses}} -> 
    #     IO.puts("Test get 5 - #{inspect(responses)}")
    # end
    # receive do 
    #   {sender, {:put, :ok, key}} -> 
    #     IO.puts("Test put 6 - #{inspect(key)}")

    #   {sender, {:get, key, responses}} -> 
    #     IO.puts("Test get 6 - #{inspect(responses)}")
    # end
    # receive do 
    #   {sender, {:put, :ok, key}} -> 
    #     IO.puts("Test put 7 - #{inspect(key)}")

    #   {sender, {:get, key, responses}} -> 
    #     IO.puts("Test get 7 - #{inspect(responses)}")
    # end
    #     Process.sleep(1000)
    #   end
    # )

    #   Process.sleep(100)
    #   tester3 = 
    #   spawn(:tester3, fn ->
    #     IO.puts("Hello")
    #     send(:client, %Dynamo.ToClientPutMessage{
    #       key: "key", 
    #       value: 2, 
    #       metadata: nil, 
    #       node_list: nil
    #     })
    #     # Process.sleep(20)
    #     # send(:g, %Dynamo.ToClientGetMessage{
    #     #   key: "key", 
    #     #   metadata: nil, 
    #     #   node_list: nil})

    #     # Process.sleep(20)
    #     # send(:g, %Dynamo.ToClientPutMessage{
    #     #   key: "key", 
    #     #   value: 4, 
    #     #   metadata: nil, 
    #     #   node_list: nil})

    #       receive do 
    #         {sender, {:put, :ok, key}} -> 
    #           IO.puts("Test put 3 - #{inspect(key)}")

    #         {sender, {:get, key, responses}} -> 
    #           IO.puts("Test get 3 - #{inspect(responses)}")
    #       end
    #       Process.sleep(1000)
    #     end
    #   )

    #   Process.sleep(100)
    #   # tester4 = 
    #   # spawn(:tester4, fn ->
    #   #   # IO.puts("Hello")
    #   #   # send(:g, %Dynamo.ToClientPutMessage{
    #   #   #   key: "key", 
    #   #   #   value: 1, 
    #   #   #   metadata: nil, 
    #   #   #   node_list: nil
    #   #   # })
    #   #   # Process.sleep(20)
    #   #   send(:client, %Dynamo.ToClientGetMessage{
    #   #     key: "key", 
    #   #     metadata: nil, 
    #   #     node_list: nil})

    #   #   # Process.sleep(20)
    #   #   # send(:g, %Dynamo.ToClientPutMessage{
    #   #   #   key: "key", 
    #   #   #   value: 4, 
    #   #   #   metadata: nil, 
    #   #   #   node_list: nil})

    #   #     receive do 
    #   #       {sender, {:put, :ok, key}} -> 
    #   #         IO.puts("Test put 4 - #{inspect(key)}")

    #   #       {sender, {:get, key, responses}} -> 
    #   #         IO.puts("Test get 4 - #{inspect(responses)}")
    #   #     end
    #   #     Process.sleep(1000)
    #   #   end
    #   # )

    #   # Process.sleep(100)

    #   tester5 = 
    #   spawn(:tester5, fn ->
    #     # IO.puts("Hello")
    #     send(:client, %Dynamo.ToClientPutMessage{
    #       key: "key", 
    #       value: 4, 
    #       metadata: nil, 
    #       node_list: nil
    #     })
    #     # Process.sleep(20)
    #     # send(:client, %Dynamo.ToClientGetMessage{
    #     #   key: "key", 
    #     #   metadata: nil, 
    #     #   node_list: nil})

    #     # Process.sleep(20)
    #     # send(:g, %Dynamo.ToClientPutMessage{
    #     #   key: "key", 
    #     #   value: 4, 
    #     #   metadata: nil, 
    #     #   node_list: nil})

    #       receive do 
    #         {sender, {:put, :ok, key}} -> 
    #           IO.puts("Test put 5 - #{inspect(key)}")

    #         {sender, {:get, key, responses}} -> 
    #           IO.puts("Test get 5 - #{inspect(responses)}")
    #       end

    #       Process.sleep(1000)

    #     end
    #   )

    #   # Process.sleep(10)
    #   tester4 = 
    #   spawn(:tester4, fn ->
    #     IO.puts("Hello")
    #     # send(:client, %Dynamo.ToClientPutMessage{
    #     #   key: "key", 
    #     #   value: 1, 
    #     #   metadata: nil, 
    #     #   node_list: nil
    #     # })
    #     Process.sleep(20)
    #     send(:client, %Dynamo.ToClientGetMessage{
    #       key: "key", 
    #       metadata: nil, 
    #       node_list: nil})

    #     # Process.sleep(20)
    #     # send(:g, %Dynamo.ToClientPutMessage{
    #     #   key: "key", 
    #     #   value: 4, 
    #     #   metadata: nil, 
    #     #   node_list: nil})

    #       receive do 
    #         {sender, {:put, :ok, key}} -> 
    #           IO.puts("Test put 4 - #{inspect(key)}")

    #         {sender, {:get, key, responses}} -> 
    #           IO.puts("Test get 4 - #{inspect(responses)}")
    #       end
    #       Process.sleep(1000)
    #     end
    #   )
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
    Process.sleep(2000)
  after
    Emulation.terminate()
  end
end
