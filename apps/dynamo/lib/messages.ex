defmodule Dynamo.Client.PutMessage do 
    alias __MODULE__
    @enforce_keys [:key, :value, :metadata, :client]
    defstruct(
        key: nil, 
        value: nil,
        metadata: nil,
        client: nil
    )

    @spec new(string(), non_neg_integer(), map(), atom()) :: 
                %PutMessage{
                    key: string(), 
                    value: non_neg_integer(), 
                    metadata: map(), 
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

    @spec new(string(), map(), atom()) :: 
                %GetMessage{
                    key: string(), 
                    metadata: map(),
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

    @spec new(string(), non_neg_integer(), map(), non_neg_integer(), atom()) :: 
                %PutRequest{
                    key: string(), 
                    value: non_neg_integer(), 
                    metadata: map(), 
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

    @spec new(string(), map(), non_neg_integer(), atom()) :: 
                %GetRequest{
                    key: string(), 
                    metadata: map(),
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

    @spec new(string(), non_neg_integer(), map(), non_neg_integer()) :: 
                %GetResponse{
                    key: string(), 
                    value: non_neg_integer(),
                    metadata: map(),
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

    @spec new(string(), non_neg_integer(), map(), atom(), non_neg_integer()) :: 
                %PutResponse{
                    key: string(), 
                    value: non_neg_integer(),
                    metadata: map(),
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

