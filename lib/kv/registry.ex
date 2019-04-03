defmodule KV.Registry do
  use GenServer

  # API

  def start_link(opts) do
    server_name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, server_name, opts)
  end

  def lookup(ets_table_name, bucket_name) do
    case :ets.lookup(ets_table_name, bucket_name) do
      [{^bucket_name, pid}] -> {:ok, pid}
      [] -> :error
    end
  end

  def create(server_name, bucket_name) do
    GenServer.cast(server_name, {:create, bucket_name})
  end

  # Server

  def init(table_name) do
    ets_table_name = :ets.new(table_name, [:named_table, read_concurrency: true])
    refs = %{}
    {:ok, {ets_table_name, refs}}
  end

  def handle_cast({:create, bucket_name}, {ets_table_name, refs}) do
    case lookup(ets_table_name, bucket_name) do
      {:ok, _pid} ->
        {:noreply, {ets_table_name, refs}}
      :error ->
        {:ok, pid} = DynamicSupervisor.start_child(KV.BucketSupervisor, KV.Bucket)
        ref = Process.monitor(pid)
        refs = Map.put(refs, ref, bucket_name)
        :ets.insert(ets_table_name, {bucket_name, pid})
        {:noreply, {ets_table_name, refs}}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, {ets_table_name, refs}) do
    {bucket_name, refs} = Map.pop(refs, ref)
    :ets.delete(ets_table_name, bucket_name)
    {:noreply, {ets_table_name, refs}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end
end
