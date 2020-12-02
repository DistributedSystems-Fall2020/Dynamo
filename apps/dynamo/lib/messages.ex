defmodule Dynamo.Client.PutMessage do 
    alias __MODULE__
    @enforce_keys [:key, :metadata, :value]
    defstruct(
        key: nil, 
        metadata: nil, 
        value: nil
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


# defmodule Dynamo.Client.GetMessage do
#     @moduledoc """
#     Response for RequestVote requests.
#     """
#     alias __MODULE__
#     @enforce_keys [:key]
#     defstruct(
#       key: nil,
#       metadata: nil
#     )
  
#     @doc """
#     Create a new RequestVoteResponse.
#     """
#     @spec new(string(), map()) ::
#             %GetMessage{
#               key: string(),
#               metadata: map()
#             }
#     def new(key, metadata) do
#       %GetMessage{key: key, metadata: metadata}
#     end
#   end
