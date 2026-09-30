defmodule ExOpenAI.StreamingClient do
  use GenServer

  require Logger

  @callback handle_data(any(), any()) :: {:noreply, any()}
  @callback handle_finish(any()) :: {:noreply, any()}
  @callback handle_error(any(), any()) :: {:noreply, any()}

  defmacro __using__(_opts) do
    quote do
      @behaviour ExOpenAI.StreamingClient

      def start_link(init_args, opts \\ []) do
        GenServer.start_link(__MODULE__, init_args, opts)
      end

      def init(init_args) do
        {:ok, init_args}
      end

      def handle_cast({:data, data}, state) do
        handle_data(data, state)
      end

      def handle_cast({:error, e}, state) do
        handle_error(e, state)
      end

      def handle_cast(:finish, state) do
        handle_finish(state)
      end
    end
  end

  def start_link(streaming_client_pid_pid, convert_response_fx) do
    GenServer.start_link(__MODULE__,
      streaming_client_pid: streaming_client_pid_pid,
      convert_response_fx: convert_response_fx
    )
  end

  def init(streaming_client_pid: pid, convert_response_fx: fx) do
    {:ok, %{streaming_client_pid: pid, convert_response_fx: fx, http_error: nil, error_body: ""}}
  end

  # An error body is worth reading, not storing whole: providers answer a bad
  # request with a sentence, but a proxy can answer with a page.
  @error_body_cap 4_000

  @doc """
  Forwards the given response back to the receiver
  If receiver is a PID, will use GenServer.cast to send
  If receiver is a function, will call the function directly
  """
  def forward_response(pid, data) when is_pid(pid) do
    GenServer.cast(pid, data)
  end

  def forward_response(callback_fx, data) when is_function(callback_fx) do
    callback_fx.(data)
  end

  def handle_chunk(
        chunk,
        %{streaming_client_pid: pid_or_fx, convert_response_fx: convert_fx}
      ) do
    chunk
    |> String.trim()
    |> case do
      "[DONE]" ->
        forward_response(pid_or_fx, :finish)

      etc ->
        json =
          Jason.decode(etc)
          |> convert_fx.()

        case json do
          {:ok, res} ->
            forward_response(pid_or_fx, {:data, res})

          {:error, _err} ->
            Logger.debug("Found chunk with incomplete JSON: #{inspect(etc)}")

            forward_response(pid_or_fx, {:data, %{partial_chunk: etc}})
        end
    end
  end

  # The response to a failed request is an error document, not an SSE stream;
  # parsing it as chunks is what discarded it.
  def handle_info(%HTTPoison.AsyncChunk{chunk: chunk}, %{http_error: code} = state)
      when is_integer(code) do
    {:noreply, %{state | error_body: truncate_error_body(state.error_body <> to_string(chunk))}}
  end

  def handle_info(
        %HTTPoison.AsyncChunk{chunk: "data: [DONE]\n\n"} = chunk,
        state
      ) do
    chunk.chunk
    |> String.replace("data: ", "")
    |> handle_chunk(state)

    {:noreply, state}
  end

  def handle_info(
        %HTTPoison.AsyncChunk{chunk: chunk},
        state
      ) do

    chunk
    |> String.trim()
    |> String.split("data:")
    |> Enum.map(&String.trim/1)
    |> Enum.filter(&(&1 != ""))
    |> Enum.each(fn subchunk ->
      handle_chunk(subchunk, state)
    end)

    {:noreply, state}
  end

  def handle_info(%HTTPoison.Error{reason: reason}, %{http_error: code} = state)
      when is_integer(code) do
    Logger.error("Error after status #{code}: #{inspect(reason)}")

    forward_response(state.streaming_client_pid, {:error, http_error_message(code, state.error_body)})
    {:noreply, %{state | http_error: nil, error_body: ""}}
  end

  def handle_info(%HTTPoison.Error{reason: reason}, state) do
    Logger.error("Error: #{inspect(reason)}")

    forward_response(state.streaming_client_pid, {:error, reason})
    {:noreply, state}
  end

  # HTTPoison delivers the status BEFORE the body, so forwarding the error here --
  # as this client used to -- reports the code and throws the provider's own
  # explanation away: every failed streaming call arrived as a bare number. The
  # error is held until the body has been collected instead.
  def handle_info(%HTTPoison.AsyncStatus{code: code} = status, state) do
    Logger.debug("Connection status: #{inspect(status)}")

    if code >= 400 do
      {:noreply, %{state | http_error: code}}
    else
      {:noreply, state}
    end
  end

  def handle_info(%HTTPoison.AsyncEnd{}, %{http_error: code} = state) when is_integer(code) do
    forward_response(state.streaming_client_pid, {:error, http_error_message(code, state.error_body)})
    {:noreply, %{state | http_error: nil, error_body: ""}}
  end

  def handle_info(%HTTPoison.AsyncEnd{}, state) do
    # :finish is already sent when data ends
    # TODO: may need a separate event for this
    # forward_response(state.streaming_client_pid, :finish)

    {:noreply, state}
  end

  def handle_info(%HTTPoison.AsyncHeaders{} = headers, state) do
    Logger.debug("Connection headers: #{inspect(headers)}")
    {:noreply, state}
  end

  def handle_info(info, state) do
    Logger.debug("Unhandled info: #{inspect(info)}")
    {:noreply, state}
  end

  # Deliberately the same shape the non-streaming path already produces, so one
  # parser downstream reads both.
  defp http_error_message(code, "") do
    "received error status code: #{code}"
  end

  defp http_error_message(code, body) do
    "received error status code: #{code}, body: #{String.trim(body)}"
  end

  defp truncate_error_body(body) when byte_size(body) > @error_body_cap do
    binary_part(body, 0, @error_body_cap)
  end

  defp truncate_error_body(body), do: body
end
