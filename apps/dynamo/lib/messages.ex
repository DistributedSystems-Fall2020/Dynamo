defmodule Dynamo.Client.PutMessage do 
    alias __MODULE__
    @enforce_keys [:key, :value, :metadata, :client]
    defstruct(
        key: nil, 
        value: nil,
        metadata: nil,
        client: nil
    )

    @spec new(string(), non_neg_integer(), any(), atom()) :: 
                %PutMessage{
                    key: string(), 
                    value: non_neg_integer(), 
                    metadata: any(), 
                    client: atom()
                }
    def new(key, value, metadata, client) do 
        %PutMessage{
            key: key, 
            value: value, 
            metadata: metadata, 
            client: client
        }
    end
end

defmodule Dynamo.Client.GetMessage do 
    alias __MODULE__
    @enforce_keys [:key, :metadata]
    defstruct(
        key: nil, 
        metadata: nil, 
        client: nil
    )

    @spec new(string(), any(), atom()) :: 
                %GetMessage{
                    key: string(), 
                    metadata: any(),
                    client: atom()
                }
    def new(key, metadata, client) do 
        %GetMessage{
            key: key, 
            metadata: metadata, 
            client: client
        }
    end
end

defmodule Dynamo.PutRequest do 
    alias __MODULE__
    @enforce_keys [:key, :value, :metadata, :seq_no, :handoff]
    defstruct(
        key: nil, 
        value: nil,
        metadata: nil,
        seq_no: nil,
        handoff: nil
    )

    @spec new(string(), non_neg_integer(), any(), non_neg_integer(), atom()) :: 
                %PutRequest{
                    key: string(), 
                    value: non_neg_integer(), 
                    metadata: any(), 
                    seq_no: non_neg_integer(),
                    handoff: atom()
                }
    def new(key, value, metadata, seq_no, handoff) do 
        %PutRequest{
            key: key, 
            value: value, 
            metadata: metadata, 
            seq_no: seq_no,
            handoff: handoff
        }
    end
end

defmodule Dynamo.GetRequest do 
    alias __MODULE__
    @enforce_keys [:key, :metadata, :seq_no, :handoff]
    defstruct(
        key: nil, 
        metadata: nil, 
        seq_no: nil,
        handoff: nil
    )

    @spec new(string(), any(), non_neg_integer(), atom()) :: 
                %GetRequest{
                    key: string(), 
                    metadata: any(),
                    seq_no: non_neg_integer(),
                    handoff: atom()
                }
    def new(key, metadata, seq_no, handoff) do 
        %GetRequest{
            key: key, 
            metadata: metadata, 
            seq_no: seq_no,
            handoff: handoff
        }
    end
end

defmodule Dynamo.GetResponse do 
    alias __MODULE__
    @enforce_keys [:key, :value, :metadata, :seq_no]
    defstruct(
        key: nil, 
        value: nil,
        metadata: nil, 
        seq_no: nil
    )

    @spec new(string(), non_neg_integer(), any(), non_neg_integer()) :: 
                %GetResponse{
                    key: string(), 
                    value: non_neg_integer(),
                    metadata: any(),
                    seq_no: non_neg_integer()
                }
    def new(key, value, metadata, seq_no) do 
        %GetResponse{
            key: key, 
            value: value,
            metadata: metadata, 
            seq_no: seq_no
        }
    end
end

defmodule Dynamo.PutResponse do 
    alias __MODULE__
    @enforce_keys [:key, :value, :metadata, :status, :seq_no]
    defstruct(
        key: nil,
        value: nil,
        metadata: nil,
        status: nil,
        seq_no: nil
    )

    @spec new(string(), non_neg_integer(), any(), atom(), non_neg_integer()) :: 
                %PutResponse{
                    key: string(), 
                    value: non_neg_integer(),
                    metadata: any(),
                    status: atom(),
                    seq_no: non_neg_integer()
                }
    def new(key, value, metadata, status, seq_no) do 
        %PutResponse{
            key: key, 
            value: value,
            metadata: metadata, 
            status: status,
            seq_no: seq_no
        }
    end
end

defmodule Dynamo.ToClientPutMessage do 
    alias __MODULE__
    @enforce_keys [:key, :value, :metadata]
    defstruct(
        key: nil, 
        value: nil,
        metadata: nil, 
        node_list: nil
    )

    @spec new(string(), non_neg_integer(), any(), list()) :: 
                %ToClientPutMessage{
                    key: string(), 
                    value: non_neg_integer(), 
                    metadata: any(), 
                    node_list: list()
                }
    def new(key, value, metadata, node_list) do 
        %ToClientPutMessage{
            key: key, 
            value: value, 
            metadata: metadata, 
            node_list: node_list
        }
    end

    @spec test_struct_ToClientPutMessage() :: no_return()
    def test_struct_ToClientPutMessage() do 
        k = %Dynamo.ToClientPutMessage{
            key: "key",
            metadata: %{},
            value: 1, 
            node_list: [:a, :b, :c]
          }
        # IO.puts("Checking struct #{inspect(k)}")
    end 
end


defmodule Dynamo.ToClientGetMessage do 
    alias __MODULE__
    @enforce_keys [:key, :metadata]
    defstruct(
        key: nil, 
        metadata: nil, 
        node_list: nil
    )

    @spec new(string(), any(), list()) :: 
                %ToClientGetMessage{
                    key: string(), 
                    metadata: any(), 
                    node_list: list()
                }
    def new(key, metadata, node_list) do 
        %ToClientGetMessage{
            key: key, 
            metadata: metadata, 
            node_list: node_list
        }
    end

    @spec test_struct_ToClientGetMessage() :: no_return()
    def test_struct_ToClientGetMessage() do 
        k = %Dynamo.ToClientGetMessage{
            key: "key",
            metadata: %{}, 
            node_list: [:a, :b, :c]
          }
        # IO.puts("Checking struct #{inspect(k)}")
    end 

end



