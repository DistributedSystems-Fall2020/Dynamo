defmodule Dynamo.Client.PutMessage do 
    alias __MODULE__
    @enforce_keys [:key, :value, :metadata]
    defstruct(
        key: nil, 
        value: nil,
        metadata: nil
    )

    @spec new(string(), non_neg_integer(), map()) :: 
                %PutMessage{
                    key: string(), 
                    value: non_neg_integer(), 
                    metadata: map(), 
                }
    def new(key, value, metadata) do 
        %PutMessage{
            key: key, 
            value: value, 
            metadata: metadata, 
        }
    end
end

defmodule Dynamo.Client.GetMessage do 
    alias __MODULE__
    @enforce_keys [:key, :metadata]
    defstruct(
        key: nil, 
        metadata: nil, 
    )

    @spec new(string(), map()) :: 
                %GetMessage{
                    key: string(), 
                    metadata: map()
                }
    def new(key, metadata) do 
        %GetMessage{
            key: key, 
            metadata: metadata, 
        }
    end
end

defmodule Dynamo.PutRequest do 
    alias __MODULE__
    @enforce_keys [:key, :value, :metadata, :seqno]
    defstruct(
        key: nil, 
        value: nil,
        metadata: nil,
        seqno: nil
    )

    @spec new(string(), non_neg_integer(), map(), non_neg_integer()) :: 
                %PutRequest{
                    key: string(), 
                    value: non_neg_integer(), 
                    metadata: map(), 
                    seqno: non_neg_integer()
                }
    def new(key, value, metadata, seqno) do 
        %PutRequest{
            key: key, 
            value: value, 
            metadata: metadata, 
            seqno: seqno
        }
    end
end

defmodule Dynamo.GetRequest do 
    alias __MODULE__
    @enforce_keys [:key, :metadata, :seqno]
    defstruct(
        key: nil, 
        metadata: nil, 
        seqno: nil
    )

    @spec new(string(), map(), non_neg_integer()) :: 
                %GetRequest{
                    key: string(), 
                    metadata: map(),
                    seqno: non_neg_integer()
                }
    def new(key, metadata, seqno) do 
        %GetRequest{
            key: key, 
            metadata: metadata, 
            seqno: seqno
        }
    end
end

# @TODO change the structure
defmodule Dynamo.GetResponse do 
    alias __MODULE__
    @enforce_keys [:key, :value, :metadata, :seqno]
    defstruct(
        key: nil, 
        value: nil,
        metadata: nil, 
        seqno: nil
    )

    @spec new(string(), non_neg_integer(), map(), non_neg_integer()) :: 
                %GetResponse{
                    key: string(), 
                    value: non_neg_integer(),
                    metadata: map(),
                    seqno: non_neg_integer()
                }
    def new(key, value, metadata, seqno) do 
        %GetResponse{
            key: key, 
            value: value,
            metadata: metadata, 
            seqno: seqno
        }
    end
end

# @TODO change the structure
defmodule Dynamo.PutResponse do 
    alias __MODULE__
    @enforce_keys [:key, :value, :metadata, :status, :seqno]
    defstruct(
        key: nil,
        value: nil,
        metadata: nil,
        status: nil,
        seqno: nil
    )

    @spec new(string(), non_neg_integer(), map(), atom(), non_neg_integer()) :: 
                %PutResponse{
                    key: string(), 
                    value: non_neg_integer(),
                    metadata: map(),
                    status: atom(),
                    seqno: non_neg_integer()
                }
    def new(key, value, metadata, status, seqno) do 
        %PutResponse{
            key: key, 
            value: value,
            metadata: metadata, 
            status: status,
            seqno: seqno
        }
    end
end

