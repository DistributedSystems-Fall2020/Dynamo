defmodule Dynamo.VectorClock do
  defstruct(clock: %{})
  @before :before
  @hafter :after
  @concurrent :concurrent

  @spec compare_component(
                  non_neg_integer(),
                  non_neg_integer()) :: :before | :after | :concurrent 
    defp compare_component(c1, c2) do 
        cond do 
            c1 < c2 -> @before 
            c1 > c2 -> @hafter
            true -> @concurrent
        end
    end

  @spec update_vector_clock(%Dynamo.VectorClock{}, atom(), non_neg_integer()) ::
          %Dynamo.VectorClock{clock: map()}
  def update_vector_clock(vector_clock, node, counter) do
    clock = vector_clock.clock

    if Map.has_key?(clock, node) do
      {_, clock} =
        Map.get_and_update(clock, node, fn curr ->
          {curr, max(counter, curr)}
        end)

      %{vector_clock | clock: clock}
    else
      clock = Map.put(clock, node, counter)
      %{vector_clock | clock: clock}
    end
  end

  @spec equal_to(%Dynamo.VectorClock{}, %Dynamo.VectorClock{}) :: boolean 
  def equal_to(vector_clock_1, vector_clock_2) do 
    clock_1 = vector_clock_1.clock
    clock_2 = vector_clock_2.clock 
    Map.equal?(clock_1, clock_2)
  end

  @spec not_equal_to(%Dynamo.VectorClock{}, %Dynamo.VectorClock{}) :: boolean 
  def not_equal_to(vector_clock_1, vector_clock_2) do 
    clock_1 = vector_clock_1.clock
    clock_2 = vector_clock_2.clock 
    not Map.equal?(clock_1, clock_2)
  end

  @spec have_same_keys(map(), map()) :: boolean 
  defp have_same_keys(clock1, clock2) do 
    key_list_1 = Map.keys(clock1)
    key_list_2 = Map.keys(clock2)
    key_list_1 = Enum.sort(key_list_1)
    key_list_2 = Enum.sort(key_list_2)

    key_list_1 == key_list_2
  end 

  @spec less_than(%Dynamo.VectorClock{}, %Dynamo.VectorClock{}) :: boolean 
  def less_than(vector_clock_1, vector_clock_2) do 
    # if the keys are not the same then return false 
    # else do the Map.merge 
    clock_1 = vector_clock_1.clock 
    clock_2 = vector_clock_2.clock 
    if have_same_keys(clock_1, clock_2) do 
        campare_result = 
            Map.values(
                Map.merge(clock_1, clock_2, fn _k, c1, c2 -> compare_component(c1, c2) end)
            )
            Enum.all?(campare_result, fn x -> x == @before end)
    else 
        false 
    end
  end


  @spec less_than_equal_to(%Dynamo.VectorClock{}, %Dynamo.VectorClock{}) :: boolean 
  def less_than_equal_to(vector_clock_1, vector_clock_2) do 
    less_than(vector_clock_1, vector_clock_2) or equal_to(vector_clock_1, vector_clock_2)
  end

  @spec greater_than(%Dynamo.VectorClock{}, %Dynamo.VectorClock{}) :: boolean 
  def greater_than(vector_clock_1, vector_clock_2) do 
    less_than(vector_clock_2, vector_clock_1) 
  end

  @spec greater_than_equal_to(%Dynamo.VectorClock{}, %Dynamo.VectorClock{}) :: boolean 
  def greater_than_equal_to(vector_clock_1, vector_clock_2) do 
    greater_than(vector_clock_1, vector_clock_2) or equal_to(vector_clock_1, vector_clock_2)
  end


  @spec merge_with_res(non_neg_integer(), %Dynamo.VectorClock{}, list()) :: any()
  def merge_with_res(curr_index, clock, result) do 
    if curr_index == length(result) do 
        {result, false}
    else 
        cond do 
            less_than_equal_to(clock, Enum.at(result, curr_index)) ->
                {result, true}
            less_than(Enum.at(result, curr_index), clock) -> 
                result = List.update_at(result, curr_index, fn _ -> clock end)
                {result, true}
            true -> 
                merge_with_res(curr_index+1, clock, result)
            end
    end
  end

  @spec coalesce_helper(non_neg_integer(), list(), list()) :: list()
  defp coalesce_helper(curr_index, vector_clock_list, result) do
    if curr_index == length(vector_clock_list) do 
        result
    else
        {result, succ} = merge_with_res(0, Enum.at(vector_clock_list, curr_index), result)
        result = if succ do result else result ++ [Enum.at(vector_clock_list, curr_index)] end
        coalesce_helper(curr_index+1, vector_clock_list, result )
    end
  end 


  @spec coalesce(list()) :: list() 
  def coalesce(vector_clock_list) do 
    coalesce_helper(0,vector_clock_list,[])
  end 

  defp combine_component(c1, c2) do 
    max(c1, c2)
  end 

    
  def combine_vector_clocks(vector_clock_1, vector_clock_2) do
    clock_1 = vector_clock_1.clock
    clock_2 = vector_clock_2.clock
    clock = Map.merge(clock_1, clock_2, fn _k, c, r -> combine_component(c, r) end)
    %Dynamo.VectorClock{clock: clock}
  end

  def converge_helper(curr_index, vector_clock_list, result) do 
    if curr_index == length(vector_clock_list) do
        result 
    else
        result = combine_vector_clocks(Enum.at(vector_clock_list, curr_index), result)
        converge_helper(curr_index + 1, vector_clock_list, result)
    end
  end 

  def converge(vector_clock_list) do 
    converge_helper(0, vector_clock_list, %Dynamo.VectorClock{})
  end


  

  ###---------------- Test functions --------------###



  @spec test_coalesce_and_converge() :: no_return()
  def test_coalesce_and_converge() do 
    vc1 = %Dynamo.VectorClock{}
    vc2 = %Dynamo.VectorClock{}
    vc3 = %Dynamo.VectorClock{}

    vc1 = Dynamo.VectorClock.update_vector_clock(vc1, :a, 1)
    vc1 = Dynamo.VectorClock.update_vector_clock(vc1, :b, 2)
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :b, 3)
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :a, 2)
    vc3 = Dynamo.VectorClock.update_vector_clock(vc3, :x, 1)
    vc3 = Dynamo.VectorClock.update_vector_clock(vc3, :y, 2)
    list_clocks = [vc1] ++ [vc2] ++ [vc3]
    diverged_clocks = coalesce(list_clocks)
    IO.puts("res for coalesce - #{inspect(diverged_clocks)}")
    fir = Enum.at(diverged_clocks, 0)
    sec = Enum.at(diverged_clocks, 1)
    IO.puts("check1  - #{inspect(less_than(fir,sec))}")
    IO.puts("check2  - #{inspect(less_than(sec,fir))}")
    res = converge(diverged_clocks)
    IO.puts("res of converge  - #{inspect(res)}")
  end

  @spec test_merge_with_res() :: no_return()
  def test_merge_with_res() do
    vc1 = %Dynamo.VectorClock{}
    vc2 = %Dynamo.VectorClock{}
    vc3 = %Dynamo.VectorClock{}

    vc1 = Dynamo.VectorClock.update_vector_clock(vc1, :a, 1)
    vc1 = Dynamo.VectorClock.update_vector_clock(vc1, :b, 2)
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :b, 3)
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :a, 2)
    vc3 = Dynamo.VectorClock.update_vector_clock(vc3, :x, 1)
    vc3 = Dynamo.VectorClock.update_vector_clock(vc3, :y, 2)
    
    result =  [vc1] 

    # result =  result ++ [vc3]
    res = merge_with_res(0, vc2, result)
    IO.puts("res for merge with res - #{inspect(res)}")
  end

  @spec test_compare_component() :: no_return()
  def test_compare_component() do
   c1 = 2 
   c2 = 3 
   res = compare_component(c1, c2)    
   IO.puts("res = #{inspect(res)}")
  end

  @spec test_have_same_keys() :: no_return()
  def test_have_same_keys() do
   clock1 = %{a: 1, b: 2} 
   clock2 = %{b: 5, a: 10}
   res = have_same_keys(clock1, clock2)    
   IO.puts("res same keys = #{inspect(res)}")
   clock1 = %{a: 1, b: 2, c: 30} 
   clock2 = %{b: 5, a: 10}
   res = have_same_keys(clock1, clock2)    
   IO.puts("res same keys = #{inspect(res)}")
  end

  @spec test_less_than() :: no_return()
  def test_less_than() do
    vc1 = %Dynamo.VectorClock{}
    vc2 = %Dynamo.VectorClock{}
    vc1 = Dynamo.VectorClock.update_vector_clock(vc1, :a, 1)
    vc1 = Dynamo.VectorClock.update_vector_clock(vc1, :b, 2)
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :b, 3)
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :a, 2)
   
    res = less_than(vc1, vc2)    
    IO.puts("res less than = #{inspect(res)}")
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :c, 2)
    res = less_than(vc1, vc2)    
    IO.puts("res less than = #{inspect(res)}")
  end

  @spec test_less_than_equal_to() :: no_return()
  def test_less_than_equal_to() do
    vc1 = %Dynamo.VectorClock{}
    vc2 = %Dynamo.VectorClock{}
    vc1 = Dynamo.VectorClock.update_vector_clock(vc1, :a, 1)
    vc1 = Dynamo.VectorClock.update_vector_clock(vc1, :b, 2)
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :b, 2)
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :a, 1)
   
    res = less_than_equal_to(vc1, vc2)    
    IO.puts("res less than equal = #{inspect(res)}")
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :a, 2)
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :b, 3)
    res = less_than_equal_to(vc1, vc2)    
    IO.puts("res less than = #{inspect(res)}")

    vc1 = Dynamo.VectorClock.update_vector_clock(vc1, :a, 20)
    res = less_than_equal_to(vc1, vc2)    
    IO.puts("res less than = #{inspect(res)}")
  end

  @spec test_greater_than() :: no_return()
  def test_greater_than() do
    vc1 = %Dynamo.VectorClock{}
    vc2 = %Dynamo.VectorClock{}
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :a, 1)
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :b, 2)
    vc1 = Dynamo.VectorClock.update_vector_clock(vc1, :b, 3)
    vc1 = Dynamo.VectorClock.update_vector_clock(vc1, :a, 2)
   
    res = greater_than(vc1, vc2)    
    IO.puts("res greater than = #{inspect(res)}")
    vc1 = Dynamo.VectorClock.update_vector_clock(vc1, :c, 2)
    res = greater_than(vc1, vc2)    
    IO.puts("res greater than = #{inspect(res)}")
  end

  @spec test_greater_than_equal_to() :: no_return()
  def test_greater_than_equal_to() do
    vc1 = %Dynamo.VectorClock{}
    vc2 = %Dynamo.VectorClock{}
    vc1 = Dynamo.VectorClock.update_vector_clock(vc1, :a, 1)
    vc1 = Dynamo.VectorClock.update_vector_clock(vc1, :b, 2)
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :b, 2)
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :a, 1)
   
    res = greater_than_equal_to(vc2, vc1)    
    IO.puts("res greater than equal = #{inspect(res)}")
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :a, 2)
    vc2 = Dynamo.VectorClock.update_vector_clock(vc2, :b, 3)
    res = greater_than_equal_to(vc2, vc1)    
    IO.puts("res greater than = #{inspect(res)}")

    vc1 = Dynamo.VectorClock.update_vector_clock(vc1, :a, 20)
    res = greater_than_equal_to(vc2, vc1)    
    IO.puts("res greater than = #{inspect(res)}")
  end

  @spec test_vector_clock() :: no_return()
  def test_vector_clock() do
    metadata = %Dynamo.VectorClock{}
    metadata = Dynamo.VectorClock.update_vector_clock(metadata, :a, 1)
    IO.puts("Vector Clock : #{inspect(metadata)}")
    metadata = Dynamo.VectorClock.update_vector_clock(metadata, :a, 3)
    IO.puts("Vector Clock : #{inspect(metadata)}")
    metadata = Dynamo.VectorClock.update_vector_clock(metadata, :b, 1001)
    IO.puts("Vector Clock : #{inspect(metadata)}")
    metadata = Dynamo.VectorClock.update_vector_clock(metadata, :b, 1002)
    IO.puts("Vector Clock : #{inspect(metadata)}")
    metadata = Dynamo.VectorClock.update_vector_clock(metadata, :b, 1)
    IO.puts("Vector Clock : #{inspect(metadata)}")
    
  end


end
