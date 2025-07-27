defmodule SocketCAN.Reader do
  use GenServer

  require Logger

  @valid_opts [:name, :interface, :registry, :recv_timeout, :wait_for]

  def start_link(opts) do
    name = opts[:name] || raise ArgumentError, "option :name is required"
    opts[:interface] || raise ArgumentError, "option :interface is required"
    opts[:registry] || raise ArgumentError, "option :registry is required"

    for key <- Keyword.keys(opts) do
      if key not in @valid_opts do
        raise ArgumentError, "unknown option: #{inspect(key)}"
      end
    end

    opts = Keyword.put_new(opts, :max_retries, 5)
    opts = Keyword.put_new(opts, :recv_timeout, 1_000)

    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    interface = opts[:interface]
    registry = opts[:registry]
    recv_timeout = opts[:recv_timeout]

    state = %{
      socket: nil,
      interface: opts[:interface],
      registry: registry,
      recv_timeout: recv_timeout
    }

    schedule_next_read()

    {:ok, state, {:continue, {:open_socket, interface, opts[:max_retries] - 1}}}
  end

  @impl true
  def handle_continue({:open_socket, interface, retries}, state) do
    case {SocketCAN.Socket.open(interface), retries} do
      {{:ok, socket}, _retries} ->
        {:noreply, put_in(state.socket, socket)}

      {{:error, :enodev}, retries} when retries > 0 ->
        Logger.warning("Device #{interface} not found: retrying #{retries} more time(s)")
        Process.sleep(1000)
        {:noreply, state, {:continue, {:open_socket, interface, retries - 1}}}

      {{:error, :enodev}, retries} when retries == 0 ->
        Logger.error("Unable to open SocketCAN device #{interface}")
        {:shutdown, state}
    end
  end

  @impl true
  def handle_info(:read, state) do
    with {:ok, frame_raw} <- :socket.recv(state.socket, [], state.recv_timeout),
         {:ok, frame} <- SocketCAN.Frame.from_binary(frame_raw, with_timestamp: true) do
      # send to processes subscribed to all frames
      Registry.dispatch(state.registry, :all, fn entries ->
        for {pid, _value} <- entries do
          send(pid, {:can_frame, frame})
        end
      end)

      # send to processes subscribed to this frame id
      Registry.dispatch(state.registry, frame.id, fn entries ->
        for {pid, _value} <- entries do
          send(pid, {:can_frame, frame})
        end
      end)
    end

    schedule_next_read()

    {:noreply, state}
  end

  defp schedule_next_read(), do: send(self(), :read)
end
